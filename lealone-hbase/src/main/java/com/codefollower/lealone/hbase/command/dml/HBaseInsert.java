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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.client.Put;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.CommandRemote;
import com.codefollower.lealone.command.dml.Insert;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionInterface;
import com.codefollower.lealone.engine.UndoLogRecord;
import com.codefollower.lealone.expression.Expression;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.command.CommandProxy;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.result.HBaseSubqueryResult;
import com.codefollower.lealone.hbase.util.HBaseRegionInfo;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.util.StatementBuilder;
import com.codefollower.lealone.util.StringUtils;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;
import com.codefollower.lealone.value.ValueUuid;

public class HBaseInsert extends Insert {
    private final HBaseSession session;
    private StatementBuilder alterTable;
    private ArrayList<Column> alterColumns;
    private boolean isBatch = false;
    private final Map<String, Map<String, List<String>>> servers = New.hashMap();
    private int rowKeyColumnIndex = -1;

    public HBaseInsert(Session session) {
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
    public void setSortedInsertMode(boolean sortedInsertMode) {
        //不使用sortedInsertMode，因为只有在PageStore中才用得到
    }

    @Override
    public void setInsertFromSelect(boolean value) {
        //不使用insertFromSelect
    }

    @Override
    public void prepare() {
        if (session.getAutoCommit() && (query != null || list.size() > 1)) {
            session.setAutoCommit(false);
            isBatch = true;
        }

        super.prepare();

        if (query != null) {
            int index = -1;
            for (Column c : columns) {
                index++;
                if (c.isRowKeyColumn()) {
                    rowKeyColumnIndex = index;
                    break;
                }
            }
        }
    }

    @Override
    public int update() {
        HBaseTable table = ((HBaseTable) this.table);
        //当在Parser中解析insert语句时，如果insert中的一些字段是新的，那么会标注字段列表已修改了，
        //并且新字段的类型是未知的，只有在执行insert时再由字段值的实际类型确定字段的类型。
        if (table.isColumnsModified()) {
            alterTable = new StatementBuilder("ALTER TABLE ");
            //不能使用ALTER TABLE t ADD COLUMN(f1 int, f2 int)这样的语法，因为有可能多个RS都在执行这种语句，在Master那会有冲突
            alterTable.append(table.getSQL()).append(" ADD COLUMN IF NOT EXISTS ");

            alterColumns = New.arrayList();
        }
        try {
            int updateCount = super.update();

            if (!servers.isEmpty()) {
                List<CommandInterface> commands = New.arrayList(servers.size());
                for (Map.Entry<String, Map<String, List<String>>> e : servers.entrySet()) {
                    String server = e.getKey();
                    CommandRemote c = SessionRemotePool.getCommandRemote(session, getParameters(), server, getPlanSQL(e
                            .getValue().entrySet()));

                    commands.add(c);
                }

                updateCount += CommandParallel.executeUpdate(commands, session.getTransaction());
            }

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
            if (isBatch)
                session.commit(false);
            return updateCount;
        } catch (Exception e) {
            if (isBatch)
                session.rollback();
            throw DbException.convert(e);
        } finally {
            if (isBatch)
                session.setAutoCommit(true);
            servers.clear();
        }
    }

    protected static String getPlanSQL(Value[] values) {
        StatementBuilder buff = new StatementBuilder();
        buff.append('(');
        for (Value v : values) {
            buff.appendExceptFirst(", ");
            if (v == null) {
                buff.append("NULL");
            } else {
                buff.append(v.getSQL());
            }
        }
        buff.append(')');
        return buff.toString();
    }

    protected static String getPlanSQL(Expression[] list) {
        StatementBuilder buff = new StatementBuilder();
        buff.append('(');
        for (Expression e : list) {
            buff.appendExceptFirst(", ");
            if (e == null) {
                buff.append("DEFAULT");
            } else {
                buff.append(e.getSQL());
            }
        }
        buff.append(')');
        return buff.toString();
    }

    public String getPlanSQL(Set<Map.Entry<String, List<String>>> regions) {
        StatementBuilder buff = new StatementBuilder();
        boolean first = true;
        for (Map.Entry<String, List<String>> entry : regions) {
            if (!first) {
                buff.append(";");
                buff.append('\n');
            } else {
                first = false;
            }
            buff.append("IN THE REGION ");
            buff.append(StringUtils.quoteStringSQL(entry.getKey()));
            buff.append('\n');
            buff.append("INSERT INTO ");
            buff.append(table.getSQL()).append('(');
            buff.resetCount();
            for (Column c : columns) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(")\n");
            if (insertFromSelect) {
                buff.append("DIRECT ");
            }
            if (sortedInsertMode) {
                buff.append("SORTED ");
            }
            buff.append("VALUES ");
            int row = 0;
            if (entry.getValue().size() > 1) {
                buff.append('\n');
            }
            for (String value : entry.getValue()) {
                if (row++ > 0) {
                    buff.append(",\n");
                }
                buff.append(value);
            }
        }
        return buff.toString();
    }

    private HBaseRow createRow(Value rowKey, String value) {
        byte[] rowKeyAsBytes = HBaseUtils.toBytes(rowKey);

        HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(getTableNameAsBytes(), rowKeyAsBytes);
        if (!HBaseUtils.isLocal(session, hri)) {
            Map<String, List<String>> regions = servers.get(hri.getRegionServerURL());
            if (regions == null) {
                regions = New.hashMap();
                servers.put(hri.getRegionServerURL(), regions);
            }

            List<String> values = regions.get(hri.getRegionName());
            if (values == null) {
                values = New.arrayList();
                regions.put(hri.getRegionName(), values);
            }
            values.add(value);

            return null;
        }

        HBaseRow row = (HBaseRow) table.getTemplateRow();
        row.setRowKey(rowKey);
        row.setRegionName(hri.getRegionNameAsBytes());

        Put put;
        if (session.getTransaction() != null)
            put = new Put(HBaseUtils.toBytes(rowKey), session.getTransaction().getTransactionId());
        else
            put = new Put(HBaseUtils.toBytes(rowKey));
        row.setPut(put);

        return row;
    }

    @Override
    protected Row createRow(int columnLen, Expression[] expr, int rowId) {
        ValueString rowKey = ValueString.get(getRowKey(rowId));

        HBaseRow row = createRow(rowKey, getPlanSQL(expr));
        if (row == null)
            return null;

        Put put = row.getPut();
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

    @Override
    public void addRow(Value[] values) {
        Value rowKey = getRowKey(values);
        HBaseRow row = createRow(rowKey, getPlanSQL(values));
        if (row == null)
            return;

        Put put = row.getPut();
        Column c;
        Value v;

        setCurrentRowNumber(++rowNumber);
        for (int j = 0, len = columns.length; j < len; j++) {
            c = columns[j];
            if (!((HBaseTable) this.table).isStatic() && c.isRowKeyColumn())
                continue;
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
        table.validateConvertUpdateSequence(session, row);
        boolean done = table.fireBeforeRow(session, null, row);
        if (!done) {
            table.addRow(session, row);
            session.log(table, UndoLogRecord.INSERT, row);
            table.fireAfterRow(session, null, row, false);
        }
    }

    @Override
    protected ResultInterface getResultInterface() {
        return new HBaseSubqueryResult(session, query, 0);
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }

    public byte[] getTableNameAsBytes() {
        return ((HBaseTable) table).getTableNameAsBytes();
    }

    public Value getRowKey(Value[] values) {
        if (rowKeyColumnIndex == -1) {
            if (table.isStatic())
                return ValueUuid.getNewRandom();
            else
                throw new RuntimeException("do not find rowKey field");
        }
        return values[rowKeyColumnIndex];
    }

    public String getRowKey(int rowIndex) {
        if (!list.isEmpty() && list.get(rowIndex).length > 0) {
            int columnIndex = 0;
            for (Column c : columns) {
                if (c.isRowKeyColumn()) {
                    return list.get(rowIndex)[columnIndex].getValue(session).getString();
                }
                columnIndex++;
            }
        }
        if (table.isStatic())
            return ValueUuid.getNewRandom().getString();
        return null;
    }

}
