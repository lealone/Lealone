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
package com.codefollower.lealone.hbase.command.dml;

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.api.Trigger;
import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.dml.Merge;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Right;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionInterface;
import com.codefollower.lealone.engine.UndoLogRecord;
import com.codefollower.lealone.expression.Expression;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.hbase.command.CommandProxy;
import com.codefollower.lealone.hbase.command.HBasePrepared;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.result.HBaseSubqueryResult;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.util.StatementBuilder;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueNull;
import com.codefollower.lealone.value.ValueString;
import com.codefollower.lealone.value.ValueUuid;

public class HBaseMerge extends Merge implements HBasePrepared {
    private final HBaseSession session;
    private String regionName;
    private byte[] regionNameAsBytes;

    private StatementBuilder alterTable;
    private ArrayList<Column> alterColumns;

    private Command updateCommand;

    public HBaseMerge(Session session) {
        super(session);
        this.session = (HBaseSession) session;
    }

    @Override
    public void setCommand(Command command) {
        super.setCommand(command);
        //不设置query
        if (query != null) {
            query.setCommand(null);
        }
    }

    @Override
    public int update() {
        HBaseTable table = ((HBaseTable) this.table);
        if (table.isColumnsModified()) {
            alterTable = new StatementBuilder("ALTER TABLE ");
            //不能使用ALTER TABLE t ADD COLUMN(f1 int, f2 int)这样的语法，因为有可能多个RS都在执行这种语句，在Master那会有冲突
            alterTable.append(table.getSQL()).append(" ADD COLUMN IF NOT EXISTS ");

            alterColumns = New.arrayList();
        }
        int updateCount = update0();
        try {
            if (table.isColumnsModified()) {
                table.setColumnsModified(false);
                SessionInterface si = CommandProxy
                        .getSessionInterface(session.getOriginalProperties(), HBaseUtils.getMasterURL());
                for (Column c : alterColumns) {
                    CommandInterface ci = si.prepareCommand(alterTable + c.getCreateSQL(true), 1);
                    ci.executeUpdate();
                }
                si.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return updateCount;
    }

    private int update0() {
        int count;
        session.getUser().checkRight(table, Right.INSERT);
        session.getUser().checkRight(table, Right.UPDATE);
        setCurrentRowNumber(0);
        int columnLen = columns.length;
        if (list.size() > 0) {
            count = 0;
            for (int x = 0, size = list.size(); x < size; x++) {
                setCurrentRowNumber(x + 1);
                Expression[] expr = list.get(x);
                Row newRow = createRow(columnLen, expr, x);
                newRow.setTransactionId(getCommand().getTransaction().getTransactionId());
                merge(newRow);
                count++;
            }
        } else {
            ResultInterface rows = new HBaseSubqueryResult((HBaseSession) session, query, 0);
            count = 0;
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, true);
            table.lock(session, true, false);
            while (rows.next()) {
                count++;
                Value[] r = rows.currentRow();
                Row newRow = createRow(columnLen, r, count);
                newRow.setTransactionId(getCommand().getTransaction().getTransactionId());
                merge(newRow);
            }
            rows.close();
            table.fire(session, Trigger.UPDATE | Trigger.INSERT, false);
        }
        return count;
    }

    private void merge(Row row) {
        ArrayList<? extends ParameterInterface> k = updateCommand.getParameters();
        for (int i = 0; i < columns.length; i++) {
            Column col = columns[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null)
                v = ValueNull.INSTANCE;
            ParameterInterface p = k.get(i);
            p.setValue(v, true);
        }
        for (int i = 0; i < keys.length; i++) {
            Column col = keys[i];
            Value v = row.getValue(col.getColumnId());
            if (v == null) {
                throw DbException.get(ErrorCode.COLUMN_CONTAINS_NULL_VALUES_1, col.getSQL());
            }
            ParameterInterface p = k.get(columns.length + i);
            p.setValue(v, true);
        }
        int count = updateCommand.executeUpdate();
        if (count == 0) {
            try {
                table.validateConvertUpdateSequence(session, row);
                boolean done = table.fireBeforeRow(session, null, row);
                if (!done) {
                    table.lock(session, true, false);
                    table.addRow(session, row);
                    session.log(table, UndoLogRecord.INSERT, row);
                    table.fireAfterRow(session, null, row, false);
                }
            } catch (DbException e) {
                throw e;
            }
        } else if (count != 1) {
            throw DbException.get(ErrorCode.DUPLICATE_KEY_1, table.getSQL());
        }
    }

    protected Row createRow(int columnLen, Expression[] expr, int rowId) {
        HBaseRow row = (HBaseRow) table.getTemplateRow();
        ValueString rowKey = ValueString.get(getRowKey());
        row.setRowKey(rowKey);
        row.setRegionName(regionNameAsBytes);

        Put put;
        if (getCommand().getTransaction() != null)
            put = new Put(HBaseUtils.toBytes(rowKey), getCommand().getTransaction().getTransactionId());
        else
            put = new Put(HBaseUtils.toBytes(rowKey));
        row.setPut(put);
        Column c;
        Value v;
        Expression e;
        for (int i = 0; i < columnLen; i++) {
            c = columns[i];
            if (!((HBaseTable) this.table).isStatic() && c.isRowKeyColumn())
                continue;
            e = expr[i];
            if (e != null) {
                // e can be null (DEFAULT)
                e = e.optimize(session);
                v = e.getValue(session);
                if (c.isTypeUnknown()) {
                    c.setType(v.getType());
                    //alterTable.appendExceptFirst(", ");
                    //alterTable.append(c.getCreateSQL());
                    if (alterColumns != null)
                        alterColumns.add(c);
                }
                try {
                    v = c.convert(e.getValue(session));
                    row.setValue(c.getColumnId(), v);

                    put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(v));
                } catch (DbException ex) {
                    throw setRow(ex, rowId, getSQL(expr));
                }
            }
        }
        return row;
    }

    public Row createRow(int columnLen, Value[] values, int rowNumber) {
        HBaseRow row = (HBaseRow) table.getTemplateRow();
        ValueString rowKey = ValueString.get(getRowKey());
        row.setRowKey(rowKey);
        row.setRegionName(regionNameAsBytes);

        Put put;
        if (getCommand().getTransaction() != null)
            put = new Put(HBaseUtils.toBytes(rowKey), getCommand().getTransaction().getTransactionId());
        else
            put = new Put(HBaseUtils.toBytes(rowKey));
        row.setPut(put);
        Column c;
        Value v;

        setCurrentRowNumber(++rowNumber);
        for (int j = 0; j < columnLen; j++) {
            c = columns[j];
            int index = c.getColumnId();
            try {
                v = values[j];
                if (c.isTypeUnknown()) {
                    c.setType(v.getType());
                    if (alterColumns != null)
                        alterColumns.add(c);
                }
                v = c.convert(values[j]);
                row.setValue(index, v);

                put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(v));
            } catch (DbException ex) {
                throw setRow(ex, rowNumber, getSQL(values));
            }
        }
        return row;
    }

    @Override
    public void prepare() {
        super.prepare();

        updateCommand = session.prepareCommand(update.getPlanSQL());
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    @Override
    public byte[] getTableNameAsBytes() {
        return ((HBaseTable) table).getTableNameAsBytes();
    }

    @Override
    public String getRowKey() {
        int index = 0;
        if (!list.isEmpty() && list.get(0).length > 0) {
            for (Column c : columns) {
                if (c.isRowKeyColumn()) {
                    return list.get(0)[index].getValue(session).getString();
                }
                index++;
            }
        }
        if (table.isStatic())
            return ValueUuid.getNewRandom().getString();
        return null;
    }

    @Override
    public Value getStartRowKeyValue() {
        return ValueString.get(getRowKey());
    }

    @Override
    public Value getEndRowKeyValue() {
        return ValueString.get(getRowKey());
    }

    @Override
    public String getRegionName() {
        return regionName;
    }

    @Override
    public void setRegionName(String regionName) {
        this.regionName = regionName;
        this.regionNameAsBytes = Bytes.toBytes(regionName);
    }
}
