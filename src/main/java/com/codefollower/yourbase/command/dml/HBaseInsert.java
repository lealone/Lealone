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
package com.codefollower.yourbase.command.dml;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.h2.api.Trigger;
import com.codefollower.h2.command.CommandInterface;
import com.codefollower.h2.command.dml.Insert;
import com.codefollower.h2.command.dml.Query;
import com.codefollower.h2.engine.Right;
import com.codefollower.h2.engine.Session;
import com.codefollower.h2.expression.Expression;
import com.codefollower.h2.message.DbException;
import com.codefollower.h2.table.Column;
import com.codefollower.h2.value.Value;
import com.codefollower.yourbase.command.CommandProxy;
import com.codefollower.yourbase.table.HBaseTable;
import com.codefollower.yourbase.util.HBaseUtils;

public class HBaseInsert extends Insert {
    public HBaseInsert(Session session) {
        super(session);
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
                    if (c.isTypeUnknown())
                        c.setType(v.getType());

                    v = c.convert(v);
                    put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(v));
                }
            }
            session.getRegionServer().put(session.getRegionName(), put);

            //动态表，insert时如果字段未定义，此时可更新meta表
            //TODO 
            if (table.isModified()) {
                table.setModified(false);
                CommandInterface c = CommandProxy.getCommandInterface(session, session.getOriginalProperties(),
                        HBaseUtils.getMasterURL(), table.getCreateSQL(), null);
                c.executeUpdate();
                //session.getDatabase().update(session, table);
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
        return null;
    }

    @Override
    public boolean isDistributedSQL() {
        return true;
    }
}
