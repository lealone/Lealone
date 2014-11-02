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
package org.lealone.hbase.command.ddl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.command.ddl.CreateTable;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.Table;
import org.lealone.engine.Session;
import org.lealone.hbase.dbobject.table.HBaseTable;
import org.lealone.hbase.dbobject.table.HBaseTableEngine;
import org.lealone.util.New;

public class CreateHBaseTable extends CreateTable {

    private ArrayList<CreateColumnFamily> cfList = New.arrayList();
    private ArrayList<String> splitKeys;

    private Options tableOptions;
    private Options defaultColumnFamilyOptions;

    private Map<String, ArrayList<Column>> columnFamilyMap = New.hashMap();

    public CreateHBaseTable(Session session, Schema schema) {
        super(session, schema);
    }

    public void setTableOptions(Options tableOptions) {
        this.tableOptions = tableOptions;
    }

    public void setDefaultColumnFamilyOptions(Options defaultColumnFamilyOptions) {
        this.defaultColumnFamilyOptions = defaultColumnFamilyOptions;
    }

    public void setSplitKeys(ArrayList<String> splitKeys) {
        this.splitKeys = splitKeys;
    }

    public void addCreateColumnFamily(CreateColumnFamily cf) {
        cfList.add(cf);
    }

    @Override
    public void addColumn(Column column) {
        super.addColumn(column);
        String cf = column.getColumnFamilyName();
        if (cf == null)
            cf = "";

        ArrayList<Column> list = columnFamilyMap.get(cf);
        if (list == null) {
            list = New.arrayList();
            columnFamilyMap.put(cf, list);
        }
        list.add(column);
    }

    private byte[][] getSplitKeys() {
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
        return splitKeys;
    }

    @Override
    protected Table createTable(CreateTableData data) {
        HTableDescriptor htd = new HTableDescriptor(data.tableName);

        String defaultColumnFamilyName = null;
        if (tableOptions != null) {
            tableOptions.initOptions(htd);
            defaultColumnFamilyName = tableOptions.getDefaultColumnFamilyName();
        }

        if (!cfList.isEmpty()) {
            for (CreateColumnFamily cf : cfList) {
                if (defaultColumnFamilyName == null)
                    defaultColumnFamilyName = cf.getColumnFamilyName();
                htd.addFamily(cf.createHColumnDescriptor(defaultColumnFamilyOptions));
            }
        } else {
            defaultColumnFamilyName = HBaseTable.DEFAULT_COLUMN_FAMILY_NAME;
            HColumnDescriptor hcd = new HColumnDescriptor(defaultColumnFamilyName);
            if (defaultColumnFamilyOptions != null)
                defaultColumnFamilyOptions.initOptions(hcd);
            htd.addFamily(hcd);
        }

        if (session.getDatabase().getSettings().databaseToUpper)
            htd.setValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME, defaultColumnFamilyName.toUpperCase());
        else
            htd.setValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME, defaultColumnFamilyName);

        ArrayList<Column> list = columnFamilyMap.get("");
        if (list != null) {
            columnFamilyMap.remove("");
            ArrayList<Column> defaultColumns = columnFamilyMap.get(defaultColumnFamilyName);
            if (defaultColumns == null)
                defaultColumns = New.arrayList(list.size());
            for (Column c : list) {
                c.setColumnFamilyName(defaultColumnFamilyName);
                defaultColumns.add(c);
            }
            columnFamilyMap.put(defaultColumnFamilyName, defaultColumns);
        }

        data.schema = getSchema();
        data.tableEngine = HBaseTableEngine.class.getName();
        data.isHidden = false;
        Column rowKeyColumn = null;
        if (pkColumns != null && pkColumns.length > 0) {
            rowKeyColumn = pkColumns[0].column;
        }
        return new HBaseTable(!isDynamicTable(), data, columnFamilyMap, htd, getSplitKeys(), rowKeyColumn);
    }
}
