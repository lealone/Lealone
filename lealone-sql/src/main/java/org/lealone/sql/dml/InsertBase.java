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
package org.lealone.sql.dml;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.SessionStatus;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.sql.PreparedSQLStatement;
import org.lealone.sql.executor.YieldableBase;
import org.lealone.sql.executor.YieldableLoopUpdateBase;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.query.Query;
import org.lealone.storage.replication.ReplicationConflictType;

public abstract class InsertBase extends ManipulationStatement {

    protected Table table;
    protected Column[] columns;
    protected Query query;
    protected boolean insertFromSelect;
    protected final ArrayList<Expression[]> list = new ArrayList<>();

    public InsertBase(ServerSession session) {
        super(session);
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void setColumns(Column[] columns) {
        this.columns = columns;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public void setInsertFromSelect(boolean value) {
        this.insertFromSelect = value;
    }

    public void addRow(Expression[] expr) {
        list.add(expr);
    }

    @Override
    public void setLocal(boolean local) {
        super.setLocal(local);
        if (query != null)
            query.setLocal(local);
    }

    @Override
    public int getPriority() {
        if (getCurrentRowNumber() > 0)
            return priority;

        if (query != null || list.size() > 10)
            priority = NORM_PRIORITY - 1;
        else
            priority = MAX_PRIORITY;
        return priority;
    }

    protected void getValuesPlanSQL(StatementBuilder buff) {
        if (list.size() > 0) {
            buff.append("VALUES ");
            int row = 0;
            if (list.size() > 1) {
                buff.append('\n');
            }
            for (Expression[] expr : list) {
                if (row++ > 0) {
                    buff.append(",\n");
                }
                buff.append('(');
                buff.resetCount();
                for (Expression e : expr) {
                    buff.appendExceptFirst(", ");
                    if (e == null) {
                        buff.append("DEFAULT");
                    } else {
                        buff.append(e.getSQL());
                    }
                }
                buff.append(')');
            }
        } else {
            buff.append(query.getPlanSQL());
        }
    }

    @Override
    public PreparedSQLStatement prepare() {
        if (columns == null) {
            if (list.size() > 0 && list.get(0).length == 0) {
                // special case where table is used as a sequence
                columns = new Column[0];
            } else {
                columns = table.getColumns();
            }
        }
        if (list.size() > 0) {
            for (Expression[] expr : list) {
                if (expr.length != columns.length) {
                    throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
                }
                for (int i = 0, len = expr.length; i < len; i++) {
                    Expression e = expr[i];
                    if (e != null) {
                        e = e.optimize(session);
                        if (e instanceof Parameter) {
                            Parameter p = (Parameter) e;
                            p.setColumn(columns[i]);
                        }
                        expr[i] = e;
                    }
                }
            }
        } else {
            query.prepare();
            if (query.getColumnCount() != columns.length) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }
        return this;
    }

    protected static abstract class YieldableInsertBase extends YieldableLoopUpdateBase {

        final InsertBase statement;
        final Table table;
        final int listSize;

        int index;
        Result rows;
        YieldableBase<Result> yieldableQuery;
        boolean isReplicationAppendMode;
        long startKey = -1;

        public YieldableInsertBase(InsertBase statement, AsyncHandler<AsyncResult<Integer>> asyncHandler) {
            super(statement, asyncHandler);
            this.statement = statement;
            table = statement.table;
            listSize = statement.list.size();
        }

        protected void handleReplicationAppend() {
            Index index = table.getScanIndex(session);
            if (index.tryExclusiveAppendLock(session)) {
                long startKey = index.getAndAddKey(listSize) + 1;
                session.setStartKey(startKey);
            } else {
                session.setStartKey(-1);
            }
            session.setAppendCount(listSize);
            session.setAppendIndex(index);
            session.setReplicationConflictType(ReplicationConflictType.APPEND);
            session.setStatus(SessionStatus.WAITING);
        }

        protected Row createNewRow() {
            Row newRow = table.getTemplateRow(); // newRow的长度是全表字段的个数，会>=columns的长度
            Expression[] expr = statement.list.get(index);
            int columnLen = statement.columns.length;
            for (int i = 0; i < columnLen; i++) {
                Column c = statement.columns[i];
                int index = c.getColumnId(); // 从0开始
                Expression e = expr[i];
                if (e != null) {
                    // e can be null (DEFAULT)
                    e = e.optimize(session);
                    try {
                        Value v = c.convert(e.getValue(session));
                        newRow.setValue(index, v);
                    } catch (DbException ex) {
                        throw statement.setRow(ex, this.index + 1, getSQL(expr));
                    }
                }
            }
            if (isReplicationAppendMode) {
                newRow.setKey(startKey + index);
            }
            return newRow;
        }

        protected Row createNewRow(Value[] values) {
            Row newRow = table.getTemplateRow();
            for (int i = 0, len = statement.columns.length; i < len; i++) {
                Column c = statement.columns[i];
                int index = c.getColumnId();
                try {
                    Value v = c.convert(values[i]);
                    newRow.setValue(index, v);
                } catch (DbException ex) {
                    throw statement.setRow(ex, updateCount.get() + 1, getSQL(values));
                }
            }
            if (isReplicationAppendMode) {
                newRow.setKey(startKey + index);
            }
            return newRow;
        }

        protected void addRowInternal(Row newRow) {
            table.validateConvertUpdateSequence(session, newRow);
            boolean done = table.fireBeforeRow(session, null, newRow); // INSTEAD OF触发器会返回true
            if (!done) {
                pendingOperationCount.incrementAndGet();
                table.addRow(session, newRow).onComplete(ar -> {
                    if (ar.isSucceeded()) {
                        table.fireAfterRow(session, null, newRow, false);
                    }
                    onComplete(ar);
                });
            }
        }
    }
}
