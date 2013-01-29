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

import java.util.ArrayList;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.yourbase.api.Trigger;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.dml.Insert;
import com.codefollower.yourbase.command.dml.Query;
import com.codefollower.yourbase.dbobject.Right;
import com.codefollower.yourbase.dbobject.table.Column;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.engine.SessionInterface;
import com.codefollower.yourbase.expression.Expression;
import com.codefollower.yourbase.hbase.command.CommandProxy;
import com.codefollower.yourbase.hbase.dbobject.table.HBaseTable;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.util.New;
import com.codefollower.yourbase.util.StatementBuilder;
import com.codefollower.yourbase.value.Value;

public class HBaseInsert extends Insert {
    private final HBaseSession session;

    public HBaseInsert(Session session) {
        super(session);
        this.session = (HBaseSession) session;
    }

    @Override
    public void addRow(Expression[] expr) {
        if (list.isEmpty()) {
            list.add(expr);
        } else {
            //TODO 目前不支持事务，所以类似这样的批量插入SQL是不支持的:
            //INSERT INTO t VALUES(m, n), (x, y)...
            throw DbException.getUnsupportedException("batch insert");
        }
    }

    @Override
    public void setQuery(Query query) {
        //TODO 目前不支持事务，所以类似这样的批量插入SQL是不支持的:
        //INSERT INTO t SELECT x FROM y
        throw DbException.getUnsupportedException("batch insert");
    }

    @Override
    public int update() {
        session.getUser().checkRight(table, Right.INSERT);
        table.fire(session, Trigger.INSERT, true);
        HBaseTable table = ((HBaseTable) this.table);
        StatementBuilder alterTable = null;
        ArrayList<Column> alterColumns = null;
        if (table.isModified()) {
            alterTable = new StatementBuilder("ALTER TABLE ");
            //不能使用ALTER TABLE t ADD COLUMN(f1 int, f2 int)这样的语法，因为有可能多个RS都在执行这种语句，在Master那会有冲突
            alterTable.append(table.getSQL()).append(" ADD COLUMN IF NOT EXISTS ");

            alterColumns = New.arrayList();
        }
        Put put = new Put(Bytes.toBytes(getRowKey()));
        try {
            int index = -1;
            Value v;
            Expression[] exprs = list.get(0);
            Expression e;
            for (Column c : columns) {
                index++;
                if (c.isRowKeyColumn())
                    continue;
                e = exprs[index];
                if (e != null) { // e can be null (DEFAULT)
                    e = e.optimize(session);
                    v = e.getValue(session);
                    if (c.isTypeUnknown()) {
                        c.setType(v.getType());
                        //alterTable.appendExceptFirst(", ");
                        //alterTable.append(c.getCreateSQL());
                        alterColumns.add(c);
                    }

                    v = c.convert(v);
                    put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(v));
                }
            }
            session.getRegionServer().put(session.getRegionName(), put);

            //动态表，insert时如果字段未定义，此时可更新meta表
            if (table.isModified()) {
                table.setModified(false);
                SessionInterface si = CommandProxy.getSessionInterface(session, session.getOriginalProperties(),
                        HBaseUtils.getMasterURL());
                for (Column c : alterColumns) {
                    CommandInterface ci = si.prepareCommand(alterTable + c.getCreateSQL(true), session.getFetchSize());
                    ci.executeUpdate();
                }
                si.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        rowNumber = 1;
        table.fire(session, Trigger.INSERT, false);
        return rowNumber;

    }

    @Override
    public String getTableName() {
        return table.getName();
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
            return System.currentTimeMillis() + "";
        return null;
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }
}
