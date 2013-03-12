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
package com.codefollower.lealone.hbase.command.ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.ddl.CreateTable;
import com.codefollower.lealone.command.ddl.CreateTableData;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTableEngine;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.util.New;

public class CreateHBaseTable extends CreateTable {
    private boolean ifNotExists;
    private String tableName;
    private ArrayList<CreateColumnFamily> cfList = New.arrayList();
    private ArrayList<String> splitKeys;

    private Options options;

    private Map<String, ArrayList<Column>> columnsMap = New.hashMap();
    private final CreateTableData data = new CreateTableData();

    public CreateHBaseTable(Session session, Schema schema, String tableName) {
        super(session, schema);
        this.tableName = tableName;
    }

    @Override
    public void addColumn(Column column) {
        data.columns.add(column);
        String cf = column.getColumnFamilyName();
        if (cf == null)
            cf = "";

        ArrayList<Column> list = columnsMap.get(cf);
        if (list == null) {
            list = New.arrayList();
            columnsMap.put(cf, list);
        }
        list.add(column);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setOptions(Options options) {
        this.options = options;
    }

    public void setSplitKeys(ArrayList<String> splitKeys) {
        this.splitKeys = splitKeys;
    }

    public void addCreateColumnFamily(CreateColumnFamily cf) {
        cfList.add(cf);
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_TABLE;
    }

    public int update() {
        if (!transactional) {
            session.commit(true);
        }

        if (getSchema().findTableOrView(session, tableName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
        }

        String defaultColumnFamilyName = null;
        String rowKeyName = null;
        if (options != null) {
            defaultColumnFamilyName = options.getDefaultColumnFamilyName();
            rowKeyName = options.getRowKeyName();
        }
        if (rowKeyName == null)
            rowKeyName = Options.DEFAULT_ROW_KEY_NAME;

        HTableDescriptor htd = new HTableDescriptor(tableName);
        for (CreateColumnFamily cf : cfList) {
            if (defaultColumnFamilyName == null)
                defaultColumnFamilyName = cf.getColumnFamilyName();
            htd.addFamily(cf.createHColumnDescriptor());
        }

        if (options != null) {
            options.initOptions(htd);
        }

        htd.setValue(Options.ON_ROW_KEY_NAME, rowKeyName);
        htd.setValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME, defaultColumnFamilyName.toUpperCase()); //H2默认转大写

        byte[][] splitKeys = null;
        if (this.splitKeys != null && this.splitKeys.size() > 0) {
            int size = this.splitKeys.size();
            splitKeys = new byte[size][];
            for (int i = 0; i < size; i++)
                splitKeys[i] = Bytes.toBytes(this.splitKeys.get(i));

            if (splitKeys != null && splitKeys.length > 0) {
                Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
                // Verify there are no duplicate split keys
                byte[] lastKey = null;
                for (byte[] splitKey : splitKeys) {
                    if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
                        throw new IllegalArgumentException("Empty split key must not be passed in the split keys.");
                    }
                    if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
                        throw new IllegalArgumentException("All split keys must be unique, " + "found duplicate: "
                                + Bytes.toStringBinary(splitKey) + ", " + Bytes.toStringBinary(lastKey));
                    }
                    lastKey = splitKey;
                }
            }
        }

        ArrayList<Column> list = columnsMap.get("");
        if (list != null) {
            columnsMap.remove("");
            columnsMap.put(defaultColumnFamilyName, list);
        }
        int id = getObjectId();
        HBaseTable table = new HBaseTable(session, getSchema(), id, tableName, columnsMap, data.columns, htd, splitKeys);
        table.setRowKeyName(rowKeyName);
        table.setTableEngine(HBaseTableEngine.class.getName());

        Database db = session.getDatabase();
        db.lockMeta(session);
        db.addSchemaObject(session, table);

        return 0;
    }

}
