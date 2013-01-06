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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.command.dml.Insert;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.table.Column;
import org.h2.value.Value;

import com.codefollower.yourbase.table.HBaseTable;
import com.codefollower.yourbase.util.HBaseUtils;

public class HBaseInsert extends Insert {
    private int rowNumber;

    public HBaseInsert(Session session) {
        super(session);
    }

    @Override
    public int update() {
        HBaseTable table = ((HBaseTable) this.table);
        Put put = new Put(Bytes.toBytes(getRowKey()));
        String rowKeyName = table.getRowKeyName();
        String defaultColumnFamilyName = table.getDefaultColumnFamilyName();
        byte[] cf = Bytes.toBytes(defaultColumnFamilyName);
        try {
            int index = -1;
            Value v = null;
            Expression e;
            for (Column c : columns) {
                index++;
                if (rowKeyName.equalsIgnoreCase(c.getName())) {
                    continue;
                }
                if (c.getColumnFamilyName() != null && !defaultColumnFamilyName.equalsIgnoreCase(c.getColumnFamilyName()))
                    cf = Bytes.toBytes(c.getColumnFamilyName());

                e = list.get(0)[index];
                if (e != null) {
                    // e can be null (DEFAULT)
                    e = e.optimize(session);
                    v = e.getValue(session);
                }
                if (c.isTypeUnknown()) {
                    c.setType(v.getType());
                }
                v = c.convert(v);
                put.add(cf, Bytes.toBytes(c.getName()), HBaseUtils.toBytes(v));
            }
            session.getRegionServer().put(session.getRegionName(), put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (table.isModified()) {
            table.setModified(false);
            table.getDatabase().update(session, table);
        }

        rowNumber = 1;
        return rowNumber;

    }

    @Override
    public String getTableName() {
        return table.getName();
    }

    @Override
    public String getRowKey() {
        String rowKeyName = ((HBaseTable) table).getRowKeyName();
        int index = 0;
        for (Column c : columns) {
            if (rowKeyName.equalsIgnoreCase(c.getName())) {
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
