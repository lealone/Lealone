/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.router;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.DebuggableThreadPoolExecutor;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.SQLCommand;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.dml.Select;
import org.lealone.storage.PageKey;
import org.lealone.storage.replication.ReplicationSession;

//CREATE/ALTER/DROP DATABASE语句需要在所有节点上执行
//其他与具体数据库相关的DDL语句会在数据库的目标节点上执行
//DML语句如果是sharding模式，需要进一步判断
public class SQLRouter {

    private static final ExecutorService executorService = new DebuggableThreadPoolExecutor("SQLRouter", 1,
            Runtime.getRuntime().availableProcessors(), 6000, TimeUnit.MILLISECONDS);

    private static void beginTransaction(StatementBase statement) {
        statement.getSession().getTransaction(statement);
    }

    private static void executeDistributedDefinitionStatement(StatementBase definitionStatement,
            AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        ServerSession currentSession = definitionStatement.getSession();
        Database db = currentSession.getDatabase();
        String[] hostIds = db.getHostIds();
        if (hostIds.length == 0) {
            String msg = "DB: " + db.getShortName() + ", Run Mode: " + db.getRunMode() + ", no hostIds";
            throw DbException.throwInternalError(msg);
        }
        NetNodeManager m = NetNodeManagerHolder.get();
        Set<NetNode> candidateNodes = new HashSet<>(hostIds.length);
        for (String hostId : hostIds) {
            candidateNodes.add(m.getNode(hostId));
        }
        List<String> initReplicationNodes = null;
        // 在sharding模式下执行ReplicationStatement时，需要预先为root page初始化默认的复制节点
        if (definitionStatement.isReplicationStatement() && db.isShardingMode() && !db.isStarting()) {
            List<NetNode> nodes = m.getReplicationNodes(db, new HashSet<>(0), candidateNodes);
            if (!nodes.isEmpty()) {
                initReplicationNodes = new ArrayList<>(nodes.size());
                for (NetNode e : nodes) {
                    String hostId = m.getHostId(e);
                    initReplicationNodes.add(hostId);
                }
            }
        }
        ReplicationSession rs = Database.createReplicationSession(currentSession, candidateNodes, null,
                initReplicationNodes);
        SQLCommand c = rs.createSQLCommand(definitionStatement.getSQL(), -1);
        c.executeUpdate().onComplete(asyncHandler);
    }

    public static void executeUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        int updateCount = 0;
        // CREATE/ALTER/DROP DATABASE语句在执行update时才知道涉及哪些节点
        if (statement.isDatabaseStatement()) {
            updateCount = statement.update();
        } else if (statement.isDDL() && !statement.isLocal()) {
            executeDistributedDefinitionStatement(statement, asyncHandler);
            return;
        } else if (statement.isLocal()) {
            updateCount = statement.update();
        } else if (statement.getSession().isShardingMode()) {
            updateCount = maybeExecuteDistributedUpdate(statement);
        } else {
            updateCount = statement.update();
        }
        asyncHandler.handle(new AsyncResult<>(updateCount));
    }

    private static int maybeExecuteDistributedUpdate(StatementBase statement) {
        int type = statement.getType();
        switch (type) {
        case SQLStatement.DELETE:
        case SQLStatement.UPDATE: {
            int updateCount = 0;
            Map<String, List<PageKey>> nodeToPageKeyMap = statement.getNodeToPageKeyMap();
            int size = nodeToPageKeyMap.size();
            if (size > 0) {
                updateCount = maybeExecuteDistributedUpdate(statement, nodeToPageKeyMap, size > 1);
            }
            return updateCount;
        }
        default:
            return statement.update();
        }
    }

    private static int maybeExecuteDistributedUpdate(StatementBase statement,
            Map<String, List<PageKey>> nodeToPageKeyMap, boolean isBatch) {
        beginTransaction(statement);

        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        ServerSession session = statement.getSession();

        try {
            if (!statement.isLocal() && isBatch) {
                if (session.isAutoCommit()) {
                    session.setAutoCommit(false);
                    isTopTransaction = true;
                } else {
                    isNestedTransaction = true;
                    session.addSavepoint(SQLStatement.INTERNAL_SAVEPOINT);
                }
            }

            // int updateCount = 0;
            // switch (statement.getType()) {
            // case SQLStatement.INSERT:
            // case SQLStatement.UPDATE:
            // case SQLStatement.DELETE:
            // case SQLStatement.MERGE:
            // updateCount = nestedRouter.executeUpdate(statement);
            // break;
            // :
            // }

            int updateCount = maybeExecuteDistributedUpdate(statement, nodeToPageKeyMap);
            if (isTopTransaction)
                session.asyncCommit(null);
            return updateCount;
        } catch (Exception e) {
            if (isTopTransaction)
                session.rollback();

            // 嵌套事务出错时提前rollback
            if (isNestedTransaction)
                session.rollbackToSavepoint(SQLStatement.INTERNAL_SAVEPOINT);

            throw DbException.convert(e);
        } finally {
            if (isTopTransaction)
                session.setAutoCommit(true);
        }
    }

    private static int maybeExecuteDistributedUpdate(StatementBase statement,
            Map<String, List<PageKey>> nodeToPageKeyMap) {
        int updateCount = 0;
        int size = nodeToPageKeyMap.size();
        String sql = statement.getPlanSQL(true);
        ServerSession currentSession = statement.getSession();
        Session[] sessions = new Session[size];
        SQLCommand[] commands = new SQLCommand[size];
        ArrayList<Callable<Integer>> callables = new ArrayList<>(size);
        int i = 0;
        for (Entry<String, List<PageKey>> e : nodeToPageKeyMap.entrySet()) {
            String hostId = e.getKey();
            List<PageKey> pageKeys = e.getValue();
            sessions[i] = currentSession.getNestedSession(hostId,
                    !NetNode.getLocalTcpNode().equals(NetNode.createTCP(hostId)));
            commands[i] = sessions[i].createSQLCommand(sql, Integer.MAX_VALUE);
            SQLCommand c = commands[i];
            callables.add(() -> {
                return c.executeUpdate(pageKeys).get();
            });
            i++;
        }

        try {
            ArrayList<Future<Integer>> futures = new ArrayList<>(size);
            for (Callable<Integer> callable : callables) {
                futures.add(executorService.submit(callable));
            }
            for (Future<Integer> f : futures) {
                Integer count = f.get();
                if (count != null) {
                    updateCount += count;
                }
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
        return updateCount;
    }

    public static void executeQuery(StatementBase statement, int maxRows, boolean scrollable,
            AsyncHandler<AsyncResult<Result>> asyncHandler) {
        Result result;
        if (statement.isLocal()) {
            result = statement.query(maxRows);
        } else if (statement.getSession().isShardingMode()) {
            beginTransaction(statement);
            result = maybeExecuteDistributedQuery(statement, maxRows, scrollable);
        } else {
            result = statement.query(maxRows);
        }
        asyncHandler.handle(new AsyncResult<>(result));
    }

    private static Result maybeExecuteDistributedQuery(StatementBase statement, int maxRows, boolean scrollable) {
        int type = statement.getType();
        switch (type) {
        case SQLStatement.SELECT: {
            Map<String, List<PageKey>> nodeToPageKeyMap = statement.getNodeToPageKeyMap();
            int size = nodeToPageKeyMap.size();
            if (size <= 0) {
                return new LocalResult();
            }

            ServerSession currentSession = statement.getSession();
            String sql = statement.getPlanSQL(true);
            Session[] sessions = new Session[size];
            SQLCommand[] commands = new SQLCommand[size];
            ArrayList<Callable<Result>> callables = new ArrayList<>(size);
            int i = 0;
            for (Entry<String, List<PageKey>> e : nodeToPageKeyMap.entrySet()) {
                String hostId = e.getKey();
                List<PageKey> pageKeys = e.getValue();
                sessions[i] = currentSession.getNestedSession(hostId,
                        !NetNode.getLocalTcpNode().equals(NetNode.createTCP(hostId)));
                commands[i] = sessions[i].createSQLCommand(sql, Integer.MAX_VALUE);
                SQLCommand c = commands[i];
                callables.add(() -> {
                    return c.executeQuery(maxRows, scrollable, pageKeys).get();
                });
                i++;
            }

            try {
                Select select = (Select) statement;
                if (!select.isGroupQuery() && select.getSortOrder() == null) {
                    return new SerializedResult(callables, maxRows, scrollable, select.getLimitRows());
                } else {
                    ArrayList<Future<Result>> futures = new ArrayList<>(size);
                    ArrayList<Result> results = new ArrayList<>(size);
                    for (Callable<Result> callable : callables) {
                        futures.add(executorService.submit(callable));
                    }

                    for (Future<Result> f : futures) {
                        results.add(f.get());
                    }

                    if (!select.isGroupQuery() && select.getSortOrder() != null)
                        return new SortedResult(maxRows, select.getSession(), select, results);

                    String newSQL = select.getPlanSQL(true, true);
                    Select newSelect = (Select) select.getSession().prepareStatement(newSQL, true);
                    newSelect.setLocal(true);

                    return new MergedResult(results, newSelect, select);
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        default:
            return statement.query(maxRows);
        }
    }
}
