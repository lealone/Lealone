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

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.yourbase.api.Trigger;
import com.codefollower.yourbase.command.dml.Delete;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.table.PlanItem;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.hbase.command.RowKeyConditionInfo;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTable;
import com.codefollower.yourbase.hbase.engine.HBaseSession;

//TODO 目前只能按where rowKey=???的方式删除，还不支持按family、qualifier、timestamp删除
public class HBaseDelete extends Delete {
    private final HBaseSession session;
    private Expression condition;
    private TableFilter tableFilter;

    private RowKeyConditionInfo rkci;

    public HBaseDelete(Session session) {
        super(session);
        this.session = (HBaseSession) session;
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
    public int update() {
        Table table = tableFilter.getTable();
        session.getUser().checkRight(table, Right.DELETE);
        table.fire(session, Trigger.DELETE, true);
        table.lock(session, true, false);

        setCurrentRowNumber(0);
        String rowKey = getRowKey();
        if (rowKey == null)
            return 0;

        try {
            setCurrentRowNumber(1);
            session.getRegionServer().delete(session.getRegionName(),
                    new org.apache.hadoop.hbase.client.Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        table.fire(session, Trigger.DELETE, false);
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
