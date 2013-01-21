/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.yourbase.hbase.command.dml;

import java.util.Map;
import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.dbobject.index.IndexCondition;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.dbobject.table.TableView;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.ExpressionColumn;
import com.codefollower.yourbase.expression.ValueExpression;
import com.codefollower.yourbase.hbase.command.RowKeyConditionInfo;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTable;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.util.New;
import com.codefollower.yourbase.value.Value;

public class HBaseSelect extends Select {
    private final HBaseSession session;

    private Map<String, RowKeyConditionInfo> rkciMap = New.hashMap();
    private RowKeyConditionInfo rkci;

    public HBaseSelect(Session session) {
        super(session);
        this.session = (HBaseSession) session;
    }

    @Override
    public boolean isDistributed() {
        if (!(topTableFilter.getTable().supportsColumnFamily()))
            return false;

        if (rkci == null || rkci.getStartKeysSize() < 2) {
            return false;
        }
        return true;
    }

    @Override
    public String[] getPlanSQLs() {
        if (rkci != null) {
            return rkci.getPlanSQLs();
        } else {
            return null;
        }
    }

    @Override
    public RowKeyConditionInfo getConditionInfo() {
        return rkci;
    }

    public RowKeyConditionInfo getConditionInfo(TableFilter f) {
        RowKeyConditionInfo rkci = rkciMap.get(f.getTable().getName());
        if (rkci == null) {
            rkci = new RowKeyConditionInfo((HBaseTable) f.getTable(), this);
            if (condition != null)
                condition = condition.removeRowKeyCondition(rkci, session);
            rkciMap.put(f.getTable().getName(), rkci);
        }

        return rkci;
    }

    @Override
    public String getTableName() {
        if ((topTableFilter.getTable() instanceof TableView))
            return ((TableView) topTableFilter.getTable()).getTableName();
        else

            return topTableFilter.getTable().getName();
    }

    @Override
    public String[] getRowKeys() {
        if (rkci != null) {
            return rkci.getRowKeys();
        } else {
            return null;
        }
    }

    @Override
    public boolean isDistributedSQL() {
        if (topTableFilter.getTable().isDistributedSQL())
            return true;
        return super.isDistributedSQL();
    }

    protected void prepareCondition() {
        TableFilter tf = topFilters.get(0);
        if (tf.getTable().supportsColumnFamily()) {
            if (!isSubquery()) {
                String rowKeyName = tf.getTable().getRowKeyName();
                rkci = new RowKeyConditionInfo((HBaseTable) tf.getTable(), this);
                condition = condition.removeRowKeyCondition(rkci, session);

                Column column = new Column(rowKeyName, Value.STRING);
                column.setTable(tf.getTable(), -1);
                ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), column);
                if (rkci.getCompareValueStart() != null)
                    tf.addIndexCondition(IndexCondition.get(rkci.getCompareTypeStart(), ec,
                            ValueExpression.get(rkci.getCompareValueStart())));
                if (rkci.getCompareValueStop() != null)
                    tf.addIndexCondition(IndexCondition.get(rkci.getCompareTypeStop(), ec,
                            ValueExpression.get(rkci.getCompareValueStop())));
            }
        }
        if (condition != null)
            for (TableFilter f : filters) {
                if (tf.getTable().supportsColumnFamily())
                    continue;
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
    }
}
