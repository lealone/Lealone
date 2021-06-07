/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.result.ResultTarget;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * INSERT
 * 
 * @author H2 Group
 * @author zhh
 */
public class Insert extends InsertBase {

    public Insert(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.INSERT;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("INSERT INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(")\n");
        if (insertFromSelect) {
            buff.append("DIRECT ");
        }
        getValuesPlanSQL(buff);
        return buff.toString();
    }

    @Override
    public int update() {
        YieldableInsert yieldable = new YieldableInsert(this, null);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableInsert createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableInsert(this, asyncHandler); // 统一处理单机模式、复制模式、sharding模式
    }

    private static class YieldableInsert extends YieldableInsertBase implements ResultTarget {
        public YieldableInsert(Insert statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
        }

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            session.getUser().checkRight(table, Right.INSERT);
            table.fire(session, Trigger.INSERT, true);
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                yieldableQuery = statement.query.createYieldableQuery(0, false, null,
                        statement.insertFromSelect ? this : null);
            }
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.INSERT, false);
        }

        @Override
        protected void executeLoopUpdate() {
            if (!isReplicationAppendMode && session.isReplicationMode() && table.getScanIndex(session).isAppendMode()) {
                startKey = table.getScanIndex(session).getStartKey(session.getReplicationName());
                if (startKey != -1) {
                    session.setFinalResult(true);
                    isReplicationAppendMode = true;
                } else {
                    // 在复制模式下append记录时，先获得一个rowId区间，然后需要在客户端做rowId区间冲突检测，最后再返回正确的rowId区间
                    handleReplicationAppend();
                    return;
                }
            }

            if (yieldableQuery == null) {
                while (pendingException == null && index < listSize) {
                    addRowInternal(createNewRow());
                    if (yieldIfNeeded(++index)) {
                        return;
                    }
                }
                onLoopEnd();
            } else {
                if (statement.insertFromSelect) {
                    yieldableQuery.run();
                    if (yieldableQuery.isStopped()) {
                        onLoopEnd();
                    }
                } else {
                    if (rows == null) {
                        yieldableQuery.run();
                        if (!yieldableQuery.isStopped()) {
                            return;
                        }
                        rows = yieldableQuery.getResult();
                    }
                    while (pendingException == null && rows.next()) {
                        Value[] values = rows.currentRow();
                        if (addRow(values)) {
                            return;
                        }
                    }
                    rows.close();
                    onLoopEnd();
                }
            }
        }

        // 以下实现ResultTarget接口，可以在执行查询时，边查边增加新记录
        @Override
        public boolean addRow(Value[] values) {
            addRowInternal(createNewRow(values));
            if (yieldIfNeeded(updateCount.get() + 1)) {
                return true;
            }
            return false;
        }

        @Override
        public int getRowCount() {
            return updateCount.get();
        }
    }
}
