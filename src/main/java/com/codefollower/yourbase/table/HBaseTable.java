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
package com.codefollower.yourbase.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKTableReadOnly;

import com.codefollower.h2.command.ddl.CreateTableData;
import com.codefollower.h2.engine.Session;
import com.codefollower.h2.index.Index;
import com.codefollower.h2.index.IndexType;
import com.codefollower.h2.message.DbException;
import com.codefollower.h2.result.Row;
import com.codefollower.h2.schema.Schema;
import com.codefollower.h2.table.Column;
import com.codefollower.h2.table.IndexColumn;
import com.codefollower.h2.table.TableBase;
import com.codefollower.h2.util.New;
import com.codefollower.h2.util.StatementBuilder;
import com.codefollower.h2.value.Value;

import com.codefollower.yourbase.command.ddl.Options;
import com.codefollower.yourbase.index.HBaseTableIndex;

public class HBaseTable extends TableBase {
    private static final String STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME = "CF";
    private HTableDescriptor hTableDescriptor;
    private String rowKeyName;
    private Index scanIndex;
    private Map<String, ArrayList<Column>> columnsMap;
    private boolean modified;
    private Column rowKeyColumn;
    private boolean isStaticTable;

    public boolean isStaticTable() {
        return isStaticTable;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public Column getRowKeyColumn() {
        if (rowKeyColumn == null) {
            rowKeyColumn = new Column(getRowKeyName(), true);
            rowKeyColumn.setTable(this, -2);
        }
        return rowKeyColumn;
    }

    public HBaseTable(CreateTableData data) {
        super(data);
        this.isStaticTable = true;

        Column[] cols = getColumns();
        for (Column c : cols) {
            //if (c.isPrimaryKey()) { //在Parser.parseColumn中设为false了不能这么用
            if (c.isRowKeyColumn()) {
                rowKeyColumn = c.getClone();
                rowKeyColumn.setTable(this, -2);
                rowKeyColumn.setRowKeyColumn(true);
                rowKeyName = rowKeyColumn.getName();
            }

            c.setColumnFamilyName(STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME);
        }
        if (rowKeyColumn == null) {
            throw new RuntimeException("static type table '" + data.tableName + "' not found a primary key");
        }
        setColumns(cols);
        scanIndex = new HBaseTableIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));

        hTableDescriptor = new HTableDescriptor(data.tableName);
        hTableDescriptor.setValue(Options.ON_ROW_KEY_NAME, rowKeyName);
        hTableDescriptor.setValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME, STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME);
        hTableDescriptor.addFamily(new HColumnDescriptor(STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME));
        createIfNotExists(data.session, data.tableName, hTableDescriptor, null);
    }

    public HBaseTable(Session session, Schema schema, int id, String name, Map<String, ArrayList<Column>> columnsMap,
            ArrayList<Column> columns, HTableDescriptor htd, byte[][] splitKeys) {
        super(schema, id, name, true, true, HBaseTableEngine.class.getName(), false);
        this.columnsMap = columnsMap;

        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);
        setColumns(cols);
        scanIndex = new HBaseTableIndex(this, id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
        hTableDescriptor = htd;
        createIfNotExists(session, name, htd, splitKeys);
    }

    public String getDefaultColumnFamilyName() {
        if (isStaticTable)
            return STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME;
        else
            return hTableDescriptor.getValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME);
    }

    public void setRowKeyName(String rowKeyName) {
        this.rowKeyName = rowKeyName;
    }

    public String getRowKeyName() {
        if (rowKeyName == null) {
            rowKeyName = Options.DEFAULT_ROW_KEY_NAME;
        }

        return rowKeyName;
    }

    @Override
    public void lock(Session session, boolean exclusive, boolean force) {

    }

    @Override
    public void close(Session session) {

    }

    @Override
    public void unlock(Session s) {

    }

    @Override
    public Index addIndex(Session session, String indexName, int indexId, IndexColumn[] cols, IndexType indexType,
            boolean create, String indexComment) {

        return scanIndex;
    }

    @Override
    public void removeRow(Session session, Row row) {

    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void addRow(Session session, Row row) {
    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {
        return "HBASE TABLE";
    }

    @Override
    public Index getScanIndex(Session session) {
        return scanIndex;
    }

    @Override
    public Index getUniqueIndex() {
        return scanIndex;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public boolean isLockedExclusively() {
        return false;
    }

    @Override
    public long getMaxDataModificationId() {
        return 0;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public boolean canGetRowCount() {
        return false;
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public long getRowCount(Session session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    @Override
    public String getCreateSQL() {
        if (isStaticTable)
            return super.getCreateSQL();
        StatementBuilder buff = new StatementBuilder("CREATE HBASE TABLE IF NOT EXISTS ");
        buff.append(getSQL());
        buff.append("(\n");
        buff.append("    OPTIONS(");
        boolean first = true;
        for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : hTableDescriptor.getValues().entrySet()) {
            //buff.appendExceptFirst(",");
            if (first) {
                first = false;
            } else {
                buff.append(",");
            }
            buff.append(toS(e.getKey())).append("='").append(toS(e.getValue())).append("'");
        }
        buff.append(")");
        HColumnDescriptor[] hcds = hTableDescriptor.getColumnFamilies();
        String cfName;
        if (hcds != null && hcds.length > 0) {
            for (HColumnDescriptor hcd : hcds) {
                cfName = hcd.getNameAsString();
                buff.append(",\n    COLUMN FAMILY ").append(cfName).append(" (\n");
                buff.append("        OPTIONS(");
                first = true;
                for (Entry<ImmutableBytesWritable, ImmutableBytesWritable> e : hcd.getValues().entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        buff.append(",");
                    }
                    buff.append(toS(e.getKey())).append("='").append(toS(e.getValue())).append("'");
                }
                buff.append(")"); //OPTIONS
                if (columnsMap.get(cfName) != null) {
                    for (Column column : columnsMap.get(cfName)) {
                        buff.append(",\n        ");
                        buff.append(column.getCreateSQL());
                    }
                }
                buff.append("\n    )"); //COLUMN FAMILY
            }
        }
        buff.append("\n)"); //CREATE HBASE TABLE
        return buff.toString();
    }

    @Override
    public void checkRename() {

    }

    @Override
    public Column getColumn(String columnName) {
        if (getRowKeyName().equalsIgnoreCase(columnName))
            return getRowKeyColumn();

        if (columnName.indexOf('.') == -1)
            return super.getColumn(getFullColumnName(null, columnName));
        else
            return super.getColumn(columnName);
    }

    public String getFullColumnName(String columnFamilyName, String columnName) {
        if (columnFamilyName == null)
            columnFamilyName = getDefaultColumnFamilyName();
        return columnFamilyName + "." + columnName;
    }

    public Column getColumn(String columnFamilyName, String columnName, boolean isInsert) {
        if (getRowKeyName().equalsIgnoreCase(columnName))
            return getRowKeyColumn();

        columnName = getFullColumnName(columnFamilyName, columnName);
        if (!isStaticTable && isInsert) {
            if (doesColumnExist(columnName))
                return super.getColumn(columnName);
            else { //处理动态列
                Column[] oldColumns = getColumns();
                Column[] newColumns;
                if (oldColumns == null)
                    newColumns = new Column[1];
                else
                    newColumns = new Column[oldColumns.length + 1];
                System.arraycopy(oldColumns, 0, newColumns, 0, oldColumns.length);
                Column c = new Column(columnName);
                newColumns[oldColumns.length] = c;
                setColumnsNoCheck(newColumns);
                modified = true;

                ArrayList<Column> list = columnsMap.get(c.getColumnFamilyName());
                if (list == null) {
                    list = New.arrayList();
                    columnsMap.put(c.getColumnFamilyName(), list);
                }
                list.add(c);
                //if (!list.contains(newColumns[oldColumns.length]))
                //    list.add(newColumns[oldColumns.length]);
                return c;
            }
        } else {
            return super.getColumn(columnName);
        }
    }

    @Override
    public Row getTemplateRow() {
        if (isStaticTable)
            return super.getTemplateRow();
        else
            return new Row(new Value[0], Row.MEMORY_CALCULATE);
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        super.removeChildrenAndResources(session);
        if (!database.isFromZookeeper())
            dropIfExists(session, getName());
    }

    private static void createIfNotExists(Session session, String tableName, HTableDescriptor htd, byte[][] splitKeys) {
        try {
            HMaster master = session.getMaster();
            if (master != null && master.getTableDescriptors().get(tableName) == null) {
                master.createTable(htd, splitKeys);
                try {
                    //确保表已可用
                    while (true) {
                        if (ZKTableReadOnly.isEnabledTable(master.getZooKeeperWatcher(), tableName))
                            break;
                        Thread.sleep(100);
                    }
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to HMaster.createTable");
        }
    }

    private static void dropIfExists(Session session, String tableName) {
        try {
            HMaster master = session.getMaster();
            if (master != null && master.getTableDescriptors().get(tableName) != null) {
                master.disableTable(Bytes.toBytes(tableName));
                while (true) {
                    if (ZKTableReadOnly.isDisabledTable(master.getZooKeeperWatcher(), tableName))
                        break;
                    Thread.sleep(100);
                }
                master.deleteTable(Bytes.toBytes(tableName));
            }
        } catch (Exception e) {
            throw DbException.convert(e); //Failed to HMaster.deleteTable
        }
    }

    private static String toS(ImmutableBytesWritable v) {
        return Bytes.toString(v.get());
    }
}
