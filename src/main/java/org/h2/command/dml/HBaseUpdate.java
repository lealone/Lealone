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
package org.h2.command.dml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.api.Trigger;
import org.h2.constant.ErrorCode;
import org.h2.engine.Right;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.expression.Parameter;
import org.h2.expression.RowKeyConditionInfo;
import org.h2.expression.ValueExpression;
import org.h2.message.DbException;
import org.h2.table.Column;
import org.h2.table.HBaseTable;
import org.h2.table.PlanItem;
import org.h2.table.Table;
import org.h2.table.TableFilter;
import org.h2.util.HBaseUtils;
import org.h2.util.New;
import org.h2.value.Value;

public class HBaseUpdate extends Update {

    private Expression condition;
    private TableFilter tableFilter;

    private ArrayList<Column> columns = New.arrayList();
    private HashMap<Column, Expression> expressionMap = New.hashMap();

    private RowKeyConditionInfo rkci;

    public HBaseUpdate(Session session) {
        super(session);
    }

    @Override
    public void setTableFilter(TableFilter tableFilter) {
        super.setTableFilter(tableFilter);
        this.tableFilter = tableFilter;
    }

    @Override
    public void setCondition(Expression condition) {
        super.setCondition(condition);
        this.condition = condition;
    }

    @Override
    public void setAssignment(Column column, Expression expression) {
        super.setAssignment(column, expression);
        if (expressionMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column.getName());
        }
        columns.add(column);
        expressionMap.put(column, expression);
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    @Override
    public int update() {
        Table table = tableFilter.getTable();
        session.getUser().checkRight(table, Right.UPDATE);
        table.fire(session, Trigger.UPDATE, true);
        table.lock(session, true, false);

        setCurrentRowNumber(0);
        if (getRowKey() == null)
            return 0;

        byte[] rowKey = Bytes.toBytes(getRowKey());
        try {
            setCurrentRowNumber(1);
            Result result = session.getRegionServer().get(session.getRegionName(), new Get(rowKey));
            Put put = new Put(rowKey);
            String cf = null;
            String cn = null;
            Column c = null;
            int columnCount = columns.size();
            String defaultCF = ((HBaseTable) table).getDefaultColumnFamilyName();
            Value v = null;
            for (KeyValue kv : result.raw()) {
                cf = Bytes.toString(kv.getFamily());
                cn = Bytes.toString(kv.getQualifier());

                for (int i = 0; i < columnCount; i++) {
                    c = columns.get(i);
                    if (c.getName().equalsIgnoreCase(cn) //
                            && ((c.getColumnFamilyName() == null && defaultCF.equalsIgnoreCase(cf)) //
                            || c.getColumnFamilyName() != null && c.getColumnFamilyName().equalsIgnoreCase(cf))) {

                        Expression newExpr = expressionMap.get(c);
                        if (newExpr == null || newExpr == ValueExpression.getDefault()) {
                            put.add(kv.getFamily(), kv.getQualifier(), kv.getValue());
                        } else {
                            v = newExpr.getValue(session);
                            v = c.convert(v);
                            put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp() + 1, HBaseUtils.toBytes(v));
                        }

                        break;
                    }
                }
            }

            RowMutations rm = new RowMutations(rowKey);
            //rm.add(new org.apache.hadoop.hbase.client.Delete(rowKey));
            rm.add(put);

            session.getRegionServer().mutateRow(session.getRegionName(), rm);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        table.fire(session, Trigger.UPDATE, false);
        return 1;

    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            //condition.createIndexConditions(session, tableFilter);
            rkci = new RowKeyConditionInfo((HBaseTable) tableFilter.getTable(), null);
            condition = condition.removeRowKeyCondition(rkci, session);
        }
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            e.mapColumns(tableFilter, 0);
            expressionMap.put(c, e.optimize(session));
        }
        PlanItem item = tableFilter.getBestPlanItem(session, 1);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    @Override
    public String getTableName() {
        return tableFilter.getTable().getName();
    }

    @Override
    public String getRowKey() {
        if (rkci != null)
            return rkci.getRowKey();
        else
            return null;
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }
}
