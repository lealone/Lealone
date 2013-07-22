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
package com.codefollower.lealone.hbase.dbobject.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.ddl.CreateTableData;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.index.Index;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.TableBase;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.ddl.Options;
import com.codefollower.lealone.hbase.dbobject.index.HBaseDelegateIndex;
import com.codefollower.lealone.hbase.dbobject.index.HBasePrimaryIndex;
import com.codefollower.lealone.hbase.dbobject.index.HBaseSecondaryIndex;
import com.codefollower.lealone.hbase.engine.HBaseDatabase;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.RowList;
import com.codefollower.lealone.util.New;
import com.codefollower.lealone.util.StatementBuilder;
import com.codefollower.lealone.value.Value;

public class HBaseTable extends TableBase {
    private static final String STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME = "CF";

    private final boolean isStatic;
    private final ArrayList<Index> indexes = New.arrayList();
    private final Index scanIndex;
    private final HTableDescriptor hTableDescriptor;
    private final String tableName;
    private final byte[] tableNameAsBytes;

    private String rowKeyName;
    private Column rowKeyColumn;
    private Map<String, ArrayList<Column>> columnsMap;

    private boolean isColumnsModified;

    private Database database;

    public HBaseTable(CreateTableData data) {
        super(data);
        database = data.session.getDatabase();
        isStatic = true;
        tableName = data.tableName;
        tableNameAsBytes = HBaseUtils.toBytes(tableName);

        Column[] cols = getColumns();
        for (Column c : cols) {
            c.setColumnFamilyName(STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME);
        }
        setColumns(cols);

        HTableDescriptor htd = new HTableDescriptor(data.tableName);
        htd.addFamily(new HColumnDescriptor(STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME));
        createIfNotExists(data.session, data.tableName, htd, null);

        scanIndex = new HBasePrimaryIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
        indexes.add(scanIndex);
        hTableDescriptor = null; //静态表不需要这个字段
    }

    public HBaseTable(Session session, Schema schema, int id, String name, Map<String, ArrayList<Column>> columnsMap,
            ArrayList<Column> columns, HTableDescriptor htd, byte[][] splitKeys) {
        super(schema, id, name, true, true, HBaseTableEngine.class.getName(), false);
        database = session.getDatabase();
        isStatic = false;
        tableName = name;
        tableNameAsBytes = HBaseUtils.toBytes(tableName);

        this.columnsMap = columnsMap;
        setColumns(columns.toArray(new Column[columns.size()]));

        createIfNotExists(session, name, htd, splitKeys);

        scanIndex = new HBasePrimaryIndex(this, id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
        indexes.add(scanIndex);
        hTableDescriptor = htd;
    }

    public byte[] getTableNameAsBytes() {
        return tableNameAsBytes;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public boolean isColumnsModified() {
        return isColumnsModified;
    }

    public void setColumnsModified(boolean modified) {
        this.isColumnsModified = modified;
    }

    public Column getRowKeyColumn() {
        if (rowKeyColumn == null) {
            rowKeyColumn = new Column(getRowKeyName(), true);
            rowKeyColumn.setTable(this, -2);
        }
        return rowKeyColumn;
    }

    public String getDefaultColumnFamilyName() {
        if (isStatic)
            return STATIC_TABLE_DEFAULT_COLUMN_FAMILY_NAME;
        else
            return hTableDescriptor.getValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME);
    }

    public void setRowKeyName(String rowKeyName) {
        this.rowKeyName = rowKeyName;
    }

    public String getRowKeyName() {
        if (rowKeyName == null) {
            if (isStatic) {
                for (Index idx : getIndexes()) {
                    if (idx.getIndexType().isPrimaryKey()) {
                        if (idx.getIndexColumns().length == 1) {
                            rowKeyColumn = idx.getIndexColumns()[0].column.getClone();
                            rowKeyColumn.setTable(this, -2);
                            rowKeyColumn.setRowKeyColumn(true);
                            rowKeyName = rowKeyColumn.getName();
                            break;
                        }
                    }
                }
            }
            if (rowKeyName == null)
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
        boolean isDelegateIndex = false;
        if (indexType.isPrimaryKey()) {
            for (IndexColumn c : cols) {
                Column column = c.column;
                if (column.isNullable()) {
                    throw DbException.get(ErrorCode.COLUMN_MUST_NOT_BE_NULLABLE_1, column.getName());
                }
                column.setPrimaryKey(true);
                column.setRowKeyColumn(true);
            }

            if (cols.length == 1)
                isDelegateIndex = true;
        }
        boolean isSessionTemporary = isTemporary() && !isGlobalTemporary();
        if (!isSessionTemporary) {
            database.lockMeta(session);

            if (!isDelegateIndex) {
                try {
                    HBaseSecondaryIndex.createIndexTableIfNotExists(session, indexName);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }
        Index index;
        if (isDelegateIndex)
            index = new HBaseDelegateIndex(this, indexId, indexName, cols, (HBasePrimaryIndex) scanIndex, indexType);
        else
            index = new HBaseSecondaryIndex(this, indexId, indexName, cols, indexType);

        index.setTemporary(isTemporary());
        if (index.getCreateSQL() != null) {
            index.setComment(indexComment);
            if (isSessionTemporary) {
                session.addLocalTempTableIndex(index);
            } else {
                database.addSchemaObject(session, index);
            }
        }
        setModified();
        indexes.add(index);
        return index;
    }

    private void setTransactionId(Session session, Row row) {
        HBaseSession hs = (HBaseSession) session;
        if (hs.getTransaction() != null) {
            row.setTransactionId(hs.getTransaction().getTransactionId());
        }
    }

    private void log(Session session, Row row) {
        if (((HBaseSession) session).getTransaction() != null) {
            ((HBaseRow) row).setTable(this);
            ((HBaseSession) session).log(getTableNameAsBytes(), row);
        }
    }

    @Override
    public void addRow(Session session, Row row) {
        lastModificationId = database.getNextModificationDataId();
        setTransactionId(session, row);

        int i = 0;
        try {
            for (int size = indexes.size(); i < size; i++) {
                Index index = indexes.get(i);
                index.add(session, row);
            }
            rowCount++;

            log(session, row);
        } catch (Throwable e) {
            try {
                while (--i >= 0) {
                    Index index = indexes.get(i);
                    index.remove(session, row);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            DbException de = DbException.convert(e);
            throw de;
        }
    }

    @Override
    public void updateRows(Prepared prepared, Session session, RowList rows) {
        Column[] columns = getColumns();
        int columnCount = columns.length;
        Put put;
        Column c;
        for (rows.reset(); rows.hasNext();) {
            HBaseRow o = (HBaseRow) rows.next();
            HBaseRow n = (HBaseRow) rows.next();

            o.setForUpdate(true);
            n.setRegionName(o.getRegionName());
            n.setRowKey(o.getRowKey());
            if (session.getTransaction() != null)
                put = new Put(HBaseUtils.toBytes(n.getRowKey()), session.getTransaction().getTransactionId());
            else
                put = new Put(HBaseUtils.toBytes(n.getRowKey()));
            for (int i = 0; i < columnCount; i++) {
                c = columns[i];
                put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(n.getValue(i)));
                n.setPut(put);
            }
        }
        super.updateRows(prepared, session, rows);
    }

    @Override
    public void removeRow(Session session, Row row) {
        removeRow(session, row, false);
    }

    public void removeRow(Session session, Row row, boolean isUndo) {
        lastModificationId = database.getNextModificationDataId();
        setTransactionId(session, row);

        int i = indexes.size() - 1;
        try {
            for (; i >= 0; i--) {
                Index index = indexes.get(i);
                index.remove(session, row);
            }
            rowCount--;

            if (!isUndo)
                log(session, row);
        } catch (Throwable e) {
            try {
                while (++i < indexes.size()) {
                    Index index = indexes.get(i);
                    index.add(session, row);
                }
            } catch (DbException e2) {
                // this could happen, for example on failure in the storage
                // but if that is not the case it means there is something wrong
                // with the database
                trace.error(e2, "could not undo operation");
                throw e2;
            }
            throw DbException.convert(e);
        }
    }

    @Override
    public void truncate(Session session) {

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
        return indexes;
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
        if (isStatic)
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
        throw DbException.getUnsupportedException("HBase Table"); //HBase的表不支持重命名
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
        if (!isStatic && isInsert) {
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
                isColumnsModified = true;

                ArrayList<Column> list = columnsMap.get(c.getColumnFamilyName());
                if (list == null) {
                    list = New.arrayList();
                    columnsMap.put(c.getColumnFamilyName(), list);
                }
                list.add(c);
                return c;
            }
        } else {
            return super.getColumn(columnName);
        }
    }

    @Override
    public Row getTemplateRow() {
        return new HBaseRow(new Value[columns.length], Row.MEMORY_CALCULATE);
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        int size = indexes.size();
        //删除索引后会同时从indexes数组中自动删除，所以需要copy一份出来，否则会抛出java.lang.IndexOutOfBoundsException
        ArrayList<Index> indexesCopy = new ArrayList<Index>(indexes);
        int i = 1;
        while (size > 1 && i < size) {
            Index index = indexesCopy.get(i++);
            if (!index.getIndexType().getBelongsToConstraint() && getName() != null) {
                database.removeSchemaObject(session, index);
            }
        }
        indexes.clear();
        boolean isFromZookeeper = ((HBaseDatabase) database).isFromZookeeper();
        super.removeChildrenAndResources(session);
        if (!isFromZookeeper)
            dropIfExists(session, getName());
    }

    @Override
    public String getName() { //removeChildrenAndResources会触发invalidate()，导致objectName为null，所以要覆盖它
        return tableName;
    }

    private static void createIfNotExists(Session session, String tableName, HTableDescriptor htd, byte[][] splitKeys) {
        try {
            HMaster master = ((HBaseSession) session).getMaster();
            if (master != null && !HBaseUtils.getHBaseAdmin().tableExists(tableName)) {
                HBaseUtils.getHBaseAdmin().createTable(htd, splitKeys);
                //                master.createTable(htd, splitKeys);
                //                try {
                //                    //确保表已可用
                //                    while (true) {
                //                        if (ZKTableReadOnly.isEnabledTable(master.getZooKeeperWatcher(), tableName))
                //                            break;
                //                        Thread.sleep(100);
                //                    }
                //                } catch (Exception e) {
                //                    throw DbException.convert(e);
                //                }
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, "Failed to HMaster.createTable");
        }
    }

    private static void dropIfExists(Session session, String tableName) {
        try {
            HMaster master = ((HBaseSession) session).getMaster();
            //这种方式不准确，连续运行两次时，有一次取得到值，有一次取不到
            //所以最准确的办法还是用HBaseAdmin检查
            //if (master != null && master.getTableDescriptors().get(tableName) != null) {
            if (master != null && HBaseUtils.getHBaseAdmin().tableExists(tableName)) {
                HBaseUtils.getHBaseAdmin().disableTable(tableName);
                HBaseUtils.getHBaseAdmin().deleteTable(tableName);
                //                master.disableTable(Bytes.toBytes(tableName));
                //                while (true) {
                //                    if (ZKTableReadOnly.isDisabledTable(master.getZooKeeperWatcher(), tableName))
                //                        break;
                //                    Thread.sleep(100);
                //                }
                //                master.deleteTable(Bytes.toBytes(tableName));
            }
        } catch (Exception e) {
            throw DbException.convert(e); //Failed to HMaster.deleteTable
        }
    }

    private static String toS(ImmutableBytesWritable v) {
        return Bytes.toString(v.get());
    }

    @Override
    public boolean isDistributed() {
        return true;
    }

    @Override
    public boolean supportsColumnFamily() {
        return true;
    }

    @Override
    public boolean supportsAlterColumnWithCopyData() {
        return false;
    }

    @Override
    public boolean supportsIndex() {
        return false;
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    public void addColumn(Column c) {
        ArrayList<Column> list;
        if (c.getColumnFamilyName() == null)
            c.setColumnFamilyName(getDefaultColumnFamilyName());
        list = columnsMap.get(c.getColumnFamilyName());
        if (list == null) {
            list = New.arrayList();
            columnsMap.put(c.getColumnFamilyName(), list);
        }
        list.add(c);

        Column[] cols = getColumns();
        Column[] newCols = new Column[cols.length + 1];
        System.arraycopy(cols, 0, newCols, 0, cols.length);
        newCols[cols.length] = c;

        setColumns(newCols);
    }

    public void dropColumn(Column column) {
        Column[] cols = getColumns();
        ArrayList<Column> list = new ArrayList<Column>(cols.length);
        for (Column c : cols) {
            if (!c.equals(column))
                list.add(c);

        }
        Column[] newCols = new Column[list.size()];
        newCols = list.toArray(newCols);

        rebuildColumnsMap(newCols, getDefaultColumnFamilyName());
    }

    private void rebuildColumnsMap(Column[] cols, String defaultColumnFamilyName) {
        columnsMap = New.hashMap();
        ArrayList<Column> list;
        for (Column c : cols) {
            if (c.getColumnFamilyName() == null)
                c.setColumnFamilyName(defaultColumnFamilyName);
            list = columnsMap.get(c.getColumnFamilyName());
            if (list == null) {
                list = New.arrayList();
                columnsMap.put(c.getColumnFamilyName(), list);
            }
            list.add(c);
        }

        setColumns(cols);
    }

    @Override
    public Row getRow(Session session, long key) {
        return null;
    }
}
