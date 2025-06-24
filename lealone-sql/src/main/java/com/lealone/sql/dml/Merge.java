/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.dml;

import java.util.ArrayList;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.api.Trigger;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.auth.Right;
import com.lealone.db.index.Index;
import com.lealone.db.row.Row;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.PreparedSQLStatement;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.StatementBase;
import com.lealone.sql.expression.Parameter;

/**
 * This class represents the statement
 * MERGE
 * 
 * @author H2 Group
 * @author zhh
 */
public class Merge extends MerSert {

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
    public YieldableMerge createYieldableUpdate(AsyncResultHandler<Integer> asyncHandler) {
        return new YieldableMerge(this, asyncHandler);
    }

    private static class YieldableMerge extends YieldableMerSert {

        final Merge mergeStatement;

        public YieldableMerge(Merge statement, AsyncResultHandler<Integer> asyncHandler) {
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
            return super.startInternal();
        }

        @Override
        protected void stopInternal() {
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }

        @Override
        protected void merSert(Row row) {
            ArrayList<Parameter> k = mergeStatement.update.getParameters();
            for (int i = 0; i < merSertStatement.columns.length; i++) {
                Column col = merSertStatement.columns[i];
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
                Parameter p = k.get(merSertStatement.columns.length + i);
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
