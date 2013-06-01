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

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.dml.Insert;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionInterface;
import com.codefollower.lealone.expression.Expression;
import com.codefollower.lealone.hbase.command.CommandProxy;
import com.codefollower.lealone.hbase.command.HBasePrepared;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.util.StatementBuilder;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;
import com.codefollower.lealone.value.ValueUuid;

//TODO
//可Insert多条记录，但是因为不支持事务，所以有可能出现部分Insert成功、部分Insert失败。
public class HBaseInsert extends Insert implements HBasePrepared {
    private final HBaseSession session;
    private String regionName;
    private byte[] regionNameAsBytes;

    private StatementBuilder alterTable;
    private ArrayList<Column> alterColumns;

    public HBaseInsert(Session session) {
        super(session);
        this.session = (HBaseSession) session;
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
        int updateCount = super.update();
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

    @Override
    protected Row createRow(int columnLen, Expression[] expr, int rowId) {
        HBaseRow row = (HBaseRow) table.getTemplateRow();
        ValueString rowKey = ValueString.get(getRowKey());
        row.setRowKey(rowKey);
        row.setRegionName(regionNameAsBytes);

        Put put;
        if (getCommand().getDistributedTransaction() != null)
            put = new Put(HBaseUtils.toBytes(rowKey), getCommand().getDistributedTransaction().getTransactionId());
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
        for (Column c : columns) {
            if (c.isRowKeyColumn()) {
                return list.get(0)[index].getValue(session).getString();
            }
            index++;
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
