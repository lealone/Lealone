/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.dml;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.auth.Right;
import org.lealone.db.index.Index;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.SQLStatement;
import org.lealone.sql.StatementBase;
import org.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * MERGE
 * 
 * @author H2 Group
 * @author zhh
 */
public class Merge extends InsertBase {

    private Column[] keys;
    private StatementBase update;

    public Merge(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.MERGE;
    }

    public void setKeys(Column[] keys) {
        this.keys = keys;
    }

    @Override
    public String getPlanSQL() {
        StatementBuilder buff = new StatementBuilder("MERGE INTO ");
        buff.append(table.getSQL()).append('(');
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL());
        }
        buff.append(')');
        if (keys != null) {
            buff.append(" KEY(");
            buff.resetCount();
            for (Column c : keys) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        }
        buff.append('\n');
        getValuesPlanSQL(buff);
        return buff.toString();
    }

    @Override
    public PreparedSQLStatement prepare() {
        super.prepare();
        if (keys == null) {
            Index idx = table.findPrimaryKey();
            if (idx == null) {
                throw DbException.get(ErrorCode.CONSTRAINT_NOT_FOUND_1, "PRIMARY KEY");
            }
            keys = idx.getColumns();
        }
        // 允许: INSERT INTO MergeTest VALUES()
        // 但是不允许: MERGE INTO MergeTest KEY(id) VALUES()
        if (list.size() > 0 && list.get(0).length == 0) {
            throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, keys[0].getSQL());
        }
        StatementBuilder buff = new StatementBuilder("UPDATE ");
        buff.append(table.getSQL()).append(" SET ");
        for (Column c : columns) {
            buff.appendExceptFirst(", ");
            buff.append(c.getSQL()).append("=?");
        }
        buff.append(" WHERE ");
        buff.resetCount();
        for (Column c : keys) {
            buff.appendExceptFirst(" AND ");
            buff.append(c.getSQL()).append("=?");
        }
        String sql = buff.toString();
        update = (StatementBase) session.prepareStatement(sql);
        return this;
    }

    @Override
    public int update() {
        YieldableMerge yieldable = new YieldableMerge(this, null);
        return syncExecute(yieldable);
    }

    @Override
    public YieldableMerge createYieldableUpdate(AsyncHandler<AsyncResult<Integer>> asyncHandler) {
        return new YieldableMerge(this, asyncHandler); // 统一处理单机模式、复制模式、sharding模式
    }

    private static class YieldableMerge extends YieldableInsertBase {

        final Merge mergeStatement;

        public YieldableMerge(Merge statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.mergeStatement = statement;
        }

        @Override
        protected boolean startInternal() {
            if (!table.trySharedLock(session))
                return true;
            session.getUser().checkRight(table, Right.INSERT);
            session.getUser().checkRight(table, Right.UPDATE);
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            statement.setCurrentRowNumber(0);
            if (statement.query != null) {
                yieldableQuery = statement.query.createYieldableQuery(0, false, null, null);
            }
            return false;
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }

        @Override
        protected void executeLoopUpdate() {
            if (yieldableQuery == null) {
                while (pendingException == null && index < listSize) {
                    merge(createNewRow());
                    if (yieldIfNeeded(++index)) {
                        return;
                    }
                }
                onLoopEnd();
            } else {
                if (rows == null) {
                    yieldableQuery.run();
                    if (!yieldableQuery.isStopped()) {
                        return;
                    }
                    rows = yieldableQuery.getResult();
                }
                while (pendingException == null && rows.next()) {
                    merge(createNewRow(rows.currentRow()));
                    if (yieldIfNeeded(++index)) {
                        return;
                    }
                }
                rows.close();
                onLoopEnd();
            }
        }

        private void merge(Row row) {
            ArrayList<Parameter> k = mergeStatement.update.getParameters();
            for (int i = 0; i < statement.columns.length; i++) {
                Column col = statement.columns[i];
                Value v = row.getValue(col.getColumnId());
                if (v == null)
                    v = ValueNull.INSTANCE;
                Parameter p = k.get(i);
                p.setValue(v);
            }
            for (int i = 0; i < mergeStatement.keys.length; i++) {
                Column col = mergeStatement.keys[i];
                Value v = row.getValue(col.getColumnId());
                if (v == null) {
                    throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
                }
                Parameter p = k.get(statement.columns.length + i);
                p.setValue(v);
            }
            // 先更新，如果没有记录被更新，说明是一条新的记录，接着再插入
            int count = mergeStatement.update.update();
            if (count > 0) {
                updateCount.addAndGet(count);
            } else if (count == 0) {
                addRowInternal(row);
            } else if (count != 1) {
                throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getSQL());
            }
        }
    }
}
