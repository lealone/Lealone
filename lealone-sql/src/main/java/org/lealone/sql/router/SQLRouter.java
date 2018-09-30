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

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.concurrent.DebuggableThreadPoolExecutor;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Command;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetEndpointManager;
import org.lealone.net.NetEndpointManagerHolder;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.dml.Select;
import org.lealone.storage.PageKey;
import org.lealone.storage.Storage;
import org.lealone.storage.replication.ReplicationSession;

public class SQLRouter {

    private static final ExecutorService executorService = new DebuggableThreadPoolExecutor("SQLRouter", 1,
            Runtime.getRuntime().availableProcessors(), 6000, TimeUnit.MILLISECONDS);

    private static void beginTransaction(StatementBase statement) {
        statement.getSession().getTransaction(statement);
    }

    public static int executeDatabaseStatement(IDatabase db, Session currentSession, StatementBase statement) {
        NetEndpointManager m = NetEndpointManagerHolder.get();
        Set<NetEndpoint> liveMembers = m.getLiveEndpoints();
        NetEndpoint localEndpoint = NetEndpoint.getLocalP2pEndpoint();
        liveMembers.remove(localEndpoint);
        if (liveMembers.isEmpty())
            return 0;
        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (NetEndpoint e : liveMembers) {
            String hostId = m.getHostId(e);
            sessions[i++] = currentSession.getNestedSession(hostId, true);
        }

        String sql = null;
        switch (statement.getType()) {
        case SQLStatement.CREATE_DATABASE:
            sql = db.getCreateSQL();
            break;
        case SQLStatement.DROP_DATABASE:
        case SQLStatement.ALTER_DATABASE:
            sql = statement.getSQL();
            break;
        }

        ReplicationSession rs = m.createReplicationSession(currentSession, sessions);
        Command c = null;
        try {
            c = rs.createCommand(sql, -1);
            return c.executeUpdate();
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

    private static int executeDefineStatement(StatementBase defineStatement) {
        NetEndpointManager m = NetEndpointManagerHolder.get();
        Set<NetEndpoint> liveMembers;
        Session currentSession = defineStatement.getSession();
        IDatabase db = currentSession.getDatabase();
        String[] hostIds = db.getHostIds();
        if (hostIds.length == 0) {
            throw DbException
                    .throwInternalError("DB: " + db.getShortName() + ", Run Mode: " + db.getRunMode() + ", no hostIds");
        } else {
            liveMembers = new HashSet<>(hostIds.length);
            for (String hostId : hostIds) {
                liveMembers.add(m.getEndpoint(hostId));
            }
        }
        List<String> initReplicationEndpoints = null;
        // 在sharding模式下执行ReplicationStatement时，需要预先为root page初始化默认的复制节点
        if (defineStatement.isReplicationStatement() && db.isShardingMode() && !db.isStarting()) {
            List<NetEndpoint> endpoints = m.getReplicationEndpoints(db, new HashSet<>(0), liveMembers);
            if (!endpoints.isEmpty()) {
                initReplicationEndpoints = new ArrayList<>(endpoints.size());
                for (NetEndpoint e : endpoints) {
                    String hostId = m.getHostId(e);
                    initReplicationEndpoints.add(hostId);
                }
            }
        }

        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (NetEndpoint e : liveMembers) {
            String hostId = m.getHostId(e);
            sessions[i++] = currentSession.getNestedSession(hostId, !NetEndpoint.getLocalP2pEndpoint().equals(e));
        }

        ReplicationSession rs = new ReplicationSession(sessions, initReplicationEndpoints);
        rs.setAutoCommit(currentSession.isAutoCommit());
        rs.setRpcTimeout(m.getRpcTimeout());
        Command c = null;
        try {
            c = rs.createCommand(defineStatement.getSQL(), -1);
            return c.executeUpdate();
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

    public static int executeUpdate(StatementBase statement) {
        // CREATE/ALTER/DROP DATABASE语句在执行update时才知道涉及哪些节点
        if (statement.isDatabaseStatement()) {
            return statement.update();
        }
        if (statement.isDDL() && !statement.isLocal()) {
            return executeDefineStatement(statement);
        }
        if (statement.isLocal()) {
            return statement.update();
        }
        if (statement.getSession().isShardingMode()) {
            return maybeExecuteDistributedUpdate(statement);
        }
        return statement.update();
    }

    private static int maybeExecuteDistributedUpdate(StatementBase statement) {
        int type = statement.getType();
        switch (type) {
        case SQLStatement.DELETE:
        case SQLStatement.UPDATE: {
            int updateCount = 0;
            Map<String, List<PageKey>> endpointToPageKeyMap = statement.getEndpointToPageKeyMap();
            int size = endpointToPageKeyMap.size();
            if (size > 0) {
                updateCount = maybeExecuteDistributedUpdate(statement, endpointToPageKeyMap, size > 1);
            }
            return updateCount;
        }
        default:
            return statement.update();
        }
    }

    private static int maybeExecuteDistributedUpdate(StatementBase statement,
            Map<String, List<PageKey>> endpointToPageKeyMap, boolean isBatch) {
        beginTransaction(statement);

        boolean isTopTransaction = false;
        boolean isNestedTransaction = false;
        Session session = statement.getSession();

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

            int updateCount = maybeExecuteDistributedUpdate(statement, endpointToPageKeyMap);
            if (isTopTransaction)
                session.prepareCommit();
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
            Map<String, List<PageKey>> endpointToPageKeyMap) {
        int updateCount = 0;
        int size = endpointToPageKeyMap.size();
        String sql = statement.getPlanSQL(true);
        Session currentSession = statement.getSession();
        Session[] sessions = new Session[size];
        Command[] commands = new Command[size];
        ArrayList<Callable<Integer>> callables = new ArrayList<>(size);
        int i = 0;
        for (Entry<String, List<PageKey>> e : endpointToPageKeyMap.entrySet()) {
            String hostId = e.getKey();
            // List<PageKey> pageKeys = e.getValue();
            sessions[i] = currentSession.getNestedSession(hostId,
                    !NetEndpoint.getLocalTcpEndpoint().equals(NetEndpoint.createTCP(hostId)));
            commands[i] = sessions[i].createCommand(sql, Integer.MAX_VALUE);
            Command c = commands[i];
            callables.add(() -> {
                return c.executeUpdate();
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

    public static Result executeQuery(StatementBase statement, int maxRows) {
        if (statement.isLocal()) {
            return statement.query(maxRows);
        }
        if (statement.getSession().isShardingMode()) {
            beginTransaction(statement);
            return maybeExecuteDistributedQuery(statement, maxRows);
        }
        return statement.query(maxRows);
    }

    private static Result maybeExecuteDistributedQuery(StatementBase statement, int maxRows) {
        int type = statement.getType();
        switch (type) {
        case SQLStatement.SELECT: {
            Select select = (Select) statement;
            Session currentSession = statement.getSession();
            Map<String, List<PageKey>> endpointToPageKeyMap = statement.getEndpointToPageKeyMap();
            int size = endpointToPageKeyMap.size();
            if (size > 0) {
                String sql = statement.getPlanSQL(true);

                boolean scrollable = false;
                Session[] sessions = new Session[size];
                Command[] commands = new Command[size];
                ArrayList<Callable<Result>> callables = new ArrayList<>(size);
                int i = 0;
                for (Entry<String, List<PageKey>> e : endpointToPageKeyMap.entrySet()) {
                    String hostId = e.getKey();
                    List<PageKey> pageKeys = e.getValue();
                    sessions[i] = currentSession.getNestedSession(hostId,
                            !NetEndpoint.getLocalTcpEndpoint().equals(NetEndpoint.createTCP(hostId)));
                    commands[i] = sessions[i].createCommand(sql, Integer.MAX_VALUE);
                    Command c = commands[i];
                    callables.add(() -> {
                        return c.executeQuery(maxRows, false, pageKeys);
                    });
                    i++;
                }

                try {
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
                        Select newSelect = (Select) select.getSession().prepareStatement(newSQL, true)
                                .getWrappedStatement();
                        newSelect.setLocal(true);

                        return new MergedResult(results, newSelect, select);
                    }
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
            return new LocalResult();
        }
        default:
            return statement.query(maxRows);
        }
    }

    public static void scaleIn(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldEndpoints,
            String[] newEndpoints) {
        ConcurrentUtils.submitTask("ScaleIn Endpoints", () -> {
            for (Storage storage : db.getStorages()) {
                storage.scaleIn(db, oldRunMode, newRunMode, oldEndpoints, newEndpoints);
            }
        });
    }

    public static void replicate(IDatabase db, RunMode oldRunMode, RunMode newRunMode,
            String[] newReplicationEndpoints) {
        ConcurrentUtils.submitTask("Replicate Pages", () -> {
            for (Storage storage : db.getStorages()) {
                storage.replicate(db, newReplicationEndpoints, newRunMode);
            }
        });
    }

    public static void sharding(IDatabase db, RunMode oldRunMode, RunMode newRunMode, String[] oldEndpoints,
            String[] newEndpoints) {
        ConcurrentUtils.submitTask("Sharding Pages", () -> {
            for (Storage storage : db.getStorages()) {
                storage.sharding(db, oldEndpoints, newEndpoints, newRunMode);
            }
        });
    }

}
