/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.executor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.Session;
import org.lealone.db.session.SessionStatus;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.net.NetNodeManagerHolder;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.sql.SQLCommand;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.storage.PageKey;

//CREATE/ALTER/DROP DATABASE语句需要在所有节点上执行
//其他与具体数据库相关的DDL语句会在数据库的目标节点上执行
//DML语句如果是sharding模式，需要进一步判断
public class DefaultYieldableShardingUpdate extends YieldableUpdateBase {

    private final AtomicInteger updateCount = new AtomicInteger();

    private volatile Throwable pendingException;
    private volatile boolean end;

    public DefaultYieldableShardingUpdate(StatementBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        super(statement, asyncHandler);
    }

    @Override
    protected void executeInternal() {
        if (!end && pendingException == null) {
            session.setStatus(SessionStatus.STATEMENT_RUNNING);
            executeDistributedUpdate();
        }
        if (end) {
            if (pendingException != null) {
                setPendingException(pendingException);
            } else {
                setResult(updateCount.get());
            }
            session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        }
    }

    private void executeDistributedUpdate() {
        // CREATE/ALTER/DROP DATABASE语句在执行update时才知道涉及哪些节点
        int uc = 0;
        if (statement.isDatabaseStatement()) {
            uc = statement.update();
        } else if (statement.isDDL()) {
            executeDistributedDefinitionStatement(statement);
            return;
        } else {
            switch (statement.getType()) {
            case SQLStatement.DELETE:
            case SQLStatement.UPDATE: {
                Map<List<String>, List<PageKey>> nodeToPageKeyMap = statement.getNodeToPageKeyMap();
                int size = nodeToPageKeyMap.size();
                if (size > 0) {
                    executeDistributedUpdate(nodeToPageKeyMap, size > 1);
                    return;
                }
                break;
            }
            default:
                uc = statement.update();
            }
        }
        updateCount.set(uc);
        end = true;
    }

    private void onComplete() {
        session.setStatus(SessionStatus.STATEMENT_COMPLETED);
        session.getTransactionListener().wakeUp(); // 及时唤醒
    }

    private void executeDistributedDefinitionStatement(StatementBase definitionStatement) {
        ServerSession currentSession = definitionStatement.getSession();
        Database db = currentSession.getDatabase();
        String[] hostIds = db.getHostIds();
        if (hostIds.length == 0) {
            String msg = "DB: " + db.getShortName() + ", Run Mode: " + db.getRunMode() + ", no hostIds";
            throw DbException.getInternalError(msg);
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
        Session s = db.createSession(currentSession, candidateNodes, null, initReplicationNodes);
        SQLCommand c = s.createSQLCommand(definitionStatement.getSQL(), -1);
        c.executeUpdate().onComplete(ar -> {
            if (ar.isSucceeded()) {
                updateCount.set(ar.getResult());
            } else {
                pendingException = ar.getCause();
            }
            end = true;
            onComplete();
        });
    }

    private void executeDistributedUpdate(Map<List<String>, List<PageKey>> nodeToPageKeyMap, boolean isBatch) {
        statement.getSession().getTransaction();

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
            executeDistributedUpdate(nodeToPageKeyMap, isTopTransaction, isNestedTransaction);
        } catch (Exception e) {
            rollback(isTopTransaction, isNestedTransaction);
            throw DbException.convert(e);
        }
    }

    private void commit(boolean isTopTransaction) {
        if (isTopTransaction) {
            session.asyncCommit(null);
            session.setAutoCommit(true);
        }
    }

    private void rollback(boolean isTopTransaction, boolean isNestedTransaction) {
        if (isTopTransaction)
            session.rollback();
        // 嵌套事务出错时提前rollback
        if (isNestedTransaction)
            session.rollbackToSavepoint(SQLStatement.INTERNAL_SAVEPOINT);
    }

    private void executeDistributedUpdate(Map<List<String>, List<PageKey>> nodeToPageKeyMap, boolean isTopTransaction,
            boolean isNestedTransaction) {
        // 确保只调用一次wakeUp
        AtomicBoolean wakeUp = new AtomicBoolean(false);
        String sql = statement.getPlanSQL(true);
        AtomicInteger size = new AtomicInteger(nodeToPageKeyMap.size());
        NetNodeManager m = NetNodeManagerHolder.get();
        String indexName = statement.getIndexName();
        for (Entry<List<String>, List<PageKey>> e : nodeToPageKeyMap.entrySet()) {
            if (pendingException != null) {
                break;
            }
            List<String> hostIds = e.getKey();
            List<PageKey> pageKeys = e.getValue();
            Set<NetNode> nodes = new HashSet<>(hostIds.size());
            for (String hostId : hostIds) {
                nodes.add(m.getNode(hostId));
            }
            Session s = session.getDatabase().createSession(session, nodes);
            DistributedSQLCommand c = s.createDistributedSQLCommand(sql, Integer.MAX_VALUE);
            c.executeDistributedUpdate(pageKeys, indexName).onComplete(ar -> {
                if (ar.isSucceeded()) {
                    updateCount.addAndGet(ar.getResult());
                    if (size.decrementAndGet() <= 0) {
                        end = true;
                        commit(isTopTransaction);
                    }
                } else {
                    end = true;
                    pendingException = ar.getCause();
                    rollback(isTopTransaction, isNestedTransaction);
                }
                if (end && wakeUp.compareAndSet(false, true)) {
                    onComplete();
                }
            });
        }
    }
}
