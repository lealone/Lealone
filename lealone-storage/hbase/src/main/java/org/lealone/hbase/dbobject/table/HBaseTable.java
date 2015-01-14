/*
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
package org.lealone.hbase.dbobject.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.api.ErrorCode;
import org.lealone.command.Prepared;
import org.lealone.command.ddl.CreateTableData;
import org.lealone.dbobject.index.Index;
import org.lealone.dbobject.index.IndexType;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.IndexColumn;
import org.lealone.dbobject.table.TableBase;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.hbase.command.CommandParallel;
import org.lealone.hbase.command.ddl.Options;
import org.lealone.hbase.dbobject.index.HBaseDelegateIndex;
import org.lealone.hbase.dbobject.index.HBasePrimaryIndex;
import org.lealone.hbase.dbobject.index.HBaseSecondaryIndex;
import org.lealone.hbase.engine.HBaseDatabase;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.engine.HBaseStorageEngine;
import org.lealone.hbase.metadata.MetaDataAdmin;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.result.Row;
import org.lealone.result.RowList;
import org.lealone.util.New;
import org.lealone.util.StatementBuilder;
import org.lealone.value.Value;

public class HBaseTable extends TableBase {

    public static final String DEFAULT_COLUMN_FAMILY_NAME = Bytes.toString(MetaDataAdmin.DEFAULT_COLUMN_FAMILY);

    /**
     * 使用create static table建立的表被称为静态表，静态表只有一个列族，并且列族名是CF，
     * 静态表必须事先定义表结构: 表所包含的字段及字段类型。<p>
     * 
     * 使用create [hbase|dynamic] table建立的表被称为动态表，动态表可以有多个列族，并不需要事先定义表结构，
     * 可以在insert时根据字面出现的字段名和字面值来确定新的字段名和字段类型。
     */
    private final boolean isStatic;

    private final Database database;
    private final String tableName;
    private final byte[] tableNameAsBytes;
    private final String defaultColumnFamilyName;
    private final byte[] defaultColumnFamilyNameAsBytes;

    private final HTableDescriptor hTableDescriptor;

    private final HBasePrimaryIndex scanIndex;
    private final ArrayList<Index> indexes = New.arrayList();

    private String rowKeyName;
    private Column rowKeyColumn;
    private Map<String, ArrayList<Column>> columnFamilyMap;

    private Set<String> shortColumnNameSet;
    private Map<String, Column> shortColumnNameMap;

    private boolean isColumnsModified;

    public HBaseTable(CreateTableData data) {
        this(true, data, null, null, null, null);
    }

    public HBaseTable(boolean isStatic, CreateTableData data, Map<String, ArrayList<Column>> columnFamilyMap,
            HTableDescriptor htd, byte[][] splitKeys, Column rowKeyColumn) {
        super(data);

        if (rowKeyColumn != null) {
            rowKeyColumn = rowKeyColumn.getClone();
            rowKeyColumn.setTable(this, -2);
            rowKeyColumn.setRowKeyColumn(true);
            rowKeyName = rowKeyColumn.getName();
            this.rowKeyColumn = rowKeyColumn;
        } else {
            this.rowKeyColumn = new Column(Column.ROWKEY, Value.STRING);
            this.rowKeyColumn.setTable(this, -2);
            this.rowKeyColumn.setRowKeyColumn(true);
            this.rowKeyName = Column.ROWKEY;
        }

        this.isStatic = isStatic;
        database = data.session.getDatabase();
        tableName = data.tableName;
        tableNameAsBytes = HBaseUtils.toBytes(tableName);

        if (isStatic) {
            htd = new HTableDescriptor(tableName);
            htd.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILY_NAME));

            columnFamilyMap = New.hashMap(1);
            columnFamilyMap.put(DEFAULT_COLUMN_FAMILY_NAME, data.columns);
        }

        if (isStatic)
            defaultColumnFamilyName = DEFAULT_COLUMN_FAMILY_NAME;
        else
            defaultColumnFamilyName = htd.getValue(Options.ON_DEFAULT_COLUMN_FAMILY_NAME);
        defaultColumnFamilyNameAsBytes = HBaseUtils.toBytes(defaultColumnFamilyName);

        this.columnFamilyMap = columnFamilyMap;

        hTableDescriptor = htd;

        createIfNotExists(data.session, tableName, htd, splitKeys);

        scanIndex = new HBasePrimaryIndex(this, data.id, IndexColumn.wrap(getColumns()), IndexType.createScan(false));
        indexes.add(scanIndex);
    }

    @Override
    protected void initColumns(ArrayList<Column> columns) {
        shortColumnNameSet = New.hashSet();
        shortColumnNameMap = New.hashMap();
        Column[] cols = new Column[columns.size()];
        columns.toArray(cols);

        for (Column c : cols) {
            if (c.getColumnFamilyName() == null)
                c.setColumnFamilyName(DEFAULT_COLUMN_FAMILY_NAME);

            addShortColumnName(c);
        }
        setColumns(cols);
    }

    private synchronized void addShortColumnName(Column c) {
        String name = c.getName();
        if (!shortColumnNameSet.contains(name)) {
            shortColumnNameSet.add(name);
            shortColumnNameMap.put(name, c);
        } else {
            shortColumnNameMap.remove(name);
        }
    }

    private synchronized void removeShortColumnName(Column c) {
        String name = c.getName();
        shortColumnNameSet.remove(name);
        shortColumnNameMap.remove(name);
    }

    public byte[] getTableNameAsBytes() {
        return tableNameAsBytes;
    }

    public String getDefaultColumnFamilyName() {
        return defaultColumnFamilyName;
    }

    public byte[] getDefaultColumnFamilyNameAsBytes() {
        return defaultColumnFamilyNameAsBytes;
    }

    @Override
    public boolean isStatic() {
        return isStatic;
    }

    @Override
    public boolean isColumnsModified() {
        return isColumnsModified;
    }

    @Override
    public void setColumnsModified(boolean modified) {
        this.isColumnsModified = modified;
    }

    @Override
    public Column getRowKeyColumn() {
        if (rowKeyColumn == null) {
            rowKeyColumn = new Column(getRowKeyName(), true);
            rowKeyColumn.setTable(this, -2);
        }
        return rowKeyColumn;
    }

    @Override
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
                rowKeyName = Column.ROWKEY;
        }

        return rowKeyName;
    }

    @Override
    public Column getColumn(String columnName) {
        if (database.getSettings().rowKey && Column.ROWKEY.equals(columnName))
            return getRowKeyColumn();

        if (columnName.indexOf('.') == -1) {
            Column c = shortColumnNameMap.get(columnName);
            if (c != null)
                return c;

            columnName = getFullColumnName(null, columnName);
        }

        return super.getColumn(columnName);
    }

    @Override
    public boolean doesColumnFamilyExist(String columnFamilyName) {
        return columnFamilyMap.containsKey(columnFamilyName);
    }

    @Override
    public boolean doesColumnExist(String columnName) {
        if (columnName.indexOf('.') == -1) {
            if (shortColumnNameMap.containsKey(columnName))
                return true;

            columnName = getFullColumnName(null, columnName);
        }

        return super.doesColumnExist(columnName);
    }

    @Override
    public String getFullColumnName(String columnFamilyName, String columnName) {
        if (columnFamilyName == null)
            columnFamilyName = getDefaultColumnFamilyName();
        return columnFamilyName + "." + columnName;
    }

    @Override
    public Column getColumn(String columnFamilyName, String columnName, boolean isInsert) {
        if (database.getSettings().rowKey && Column.ROWKEY.equals(columnName))
            return getRowKeyColumn();

        if (columnFamilyName == null) {
            Column c = shortColumnNameMap.get(columnName);
            if (c != null)
                return c;
        }

        columnName = getFullColumnName(columnFamilyName, columnName);
        if (!isStatic && isInsert) {
            if (doesColumnExist(columnName))
                return getColumn(columnName);
            else { //处理动态列
                Column[] oldColumns = getColumns();
                Column[] newColumns;
                if (oldColumns == null)
                    newColumns = new Column[1];
                else
                    newColumns = new Column[oldColumns.length + 1];
                System.arraycopy(oldColumns, 0, newColumns, 0, oldColumns.length);
                Column c = new Column(columnName);
                addShortColumnName(c);
                newColumns[oldColumns.length] = c;
                setColumnsNoCheck(newColumns);
                isColumnsModified = true;

                ArrayList<Column> list = columnFamilyMap.get(c.getColumnFamilyName());
                if (list == null) {
                    list = New.arrayList();
                    columnFamilyMap.put(c.getColumnFamilyName(), list);
                }
                list.add(c);
                return c;
            }
        } else {
            return getColumn(columnName);
        }
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

            if (cols.length == 1) {
                isDelegateIndex = true;
                if (isStatic) {
                    rowKeyName = null;
                    getRowKeyColumn();
                }
            }
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
            index = new HBaseDelegateIndex(this, indexId, indexName, cols, scanIndex, indexType);
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
        row.setTransactionId(session.getTransaction().getTransactionId());
    }

    private void log(Session session, Row row) {
        HBaseRow row2 = (HBaseRow) row;
        row2.setTable(this);
        ((HBaseSession) session).log(row2);
    }

    @Override
    public void addRow(final Session session, final Row row) {
        lastModificationId = database.getNextModificationDataId();
        setTransactionId(session, row);
        log(session, row);

        if (doesSecondaryIndexExist()) {
            int size = indexes.size();
            List<Callable<Void>> calls = New.arrayList(size);
            for (int i = 0; i < size; i++) {
                final Index index = indexes.get(i);
                if (!(index instanceof HBaseDelegateIndex)) {
                    calls.add(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            index.add(session, row);
                            return null;
                        }
                    });
                }
            }

            CommandParallel.execute(calls);
        } else {
            scanIndex.add(session, row);
        }

        rowCount++;
    }

    @Override
    public void removeRow(Session session, Row row) {
        removeRow(session, row, false);
    }

    @Override
    public void removeRow(final Session session, final Row row, boolean isUndo) {
        if (!isUndo) {
            lastModificationId = database.getNextModificationDataId();
            setTransactionId(session, row);
            log(session, row);
        }

        if (!isUndo && doesSecondaryIndexExist()) {
            int size = indexes.size();
            List<Callable<Void>> calls = New.arrayList(size);
            for (int i = 0; i < size; i++) {
                final Index index = indexes.get(i);
                if (!(index instanceof HBaseDelegateIndex)) {
                    calls.add(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            index.remove(session, row);
                            return null;
                        }
                    });
                }
            }

            CommandParallel.execute(calls);
        } else {
            scanIndex.remove(session, row, isUndo);
        }

        rowCount--;
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

            put = ((HBaseSession) session).getTransaction().createHBasePut(defaultColumnFamilyNameAsBytes, n.getRowKey());
            for (int i = 0; i < columnCount; i++) {
                c = columns[i];
                put.add(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes(), HBaseUtils.toBytes(n.getValue(i)));
                n.setPut(put);
            }
        }
        super.updateRows(prepared, session, rows);
    }

    @Override
    public void truncate(Session session) {

    }

    @Override
    public void checkSupportAlter() {

    }

    @Override
    public String getTableType() {
        return HBaseStorageEngine.NAME + "_" + super.getTableType();
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
        //if (isStatic)
        //    return super.getCreateSQL();
        StatementBuilder buff = new StatementBuilder("CREATE " + (isStatic ? "STATIC" : "") + " TABLE IF NOT EXISTS ");
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
                if (columnFamilyMap.get(cfName) != null) {
                    for (Column column : columnFamilyMap.get(cfName)) {
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
    public boolean supportsSharding() {
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
    public long getDiskSpaceUsed() {
        return 0;
    }

    @Override
    public void addColumn(Column c) {
        ArrayList<Column> list;
        if (c.getColumnFamilyName() == null)
            c.setColumnFamilyName(getDefaultColumnFamilyName());
        list = columnFamilyMap.get(c.getColumnFamilyName());
        if (list == null) {
            list = New.arrayList();
            columnFamilyMap.put(c.getColumnFamilyName(), list);
        }
        list.add(c);

        Column[] cols = getColumns();
        Column[] newCols = new Column[cols.length + 1];
        System.arraycopy(cols, 0, newCols, 0, cols.length);
        newCols[cols.length] = c;

        addShortColumnName(c);

        setColumns(newCols);
    }

    @Override
    public void dropColumn(Column column) {
        Column[] cols = getColumns();
        ArrayList<Column> list = new ArrayList<Column>(cols.length);
        for (Column c : cols) {
            if (!c.equals(column))
                list.add(c);

        }
        Column[] newCols = new Column[list.size()];
        newCols = list.toArray(newCols);

        removeShortColumnName(column);

        rebuildColumnsMap(newCols, getDefaultColumnFamilyName());
    }

    private void rebuildColumnsMap(Column[] cols, String defaultColumnFamilyName) {
        columnFamilyMap = New.hashMap();
        ArrayList<Column> list;
        for (Column c : cols) {
            if (c.getColumnFamilyName() == null)
                c.setColumnFamilyName(defaultColumnFamilyName);
            list = columnFamilyMap.get(c.getColumnFamilyName());
            if (list == null) {
                list = New.arrayList();
                columnFamilyMap.put(c.getColumnFamilyName(), list);
            }
            list.add(c);
        }

        setColumns(cols);
    }

    @Override
    public Row getRow(Session session, long key) {
        return null;
    }

    public boolean doesSecondaryIndexExist() {
        for (int i = indexes.size() - 1; i > 0; i--)
            if (indexes.get(i) instanceof HBaseSecondaryIndex)
                return true;

        return false;
    }

    @Override
    public boolean supportsLocalSecondaryIndex() { //目前在HBase中实现的都是全局索引
        return false;
    }

    private String primaryKeyName;

    public String getPrimaryKeyName() {
        if (primaryKeyName == null) {
            if (isStatic) {
                for (Index idx : getIndexes()) {
                    if (idx.getIndexType().isPrimaryKey()) {
                        if (idx.getIndexColumns().length == 1) {
                            primaryKeyName = idx.getIndexColumns()[0].column.getFullName();
                            break;
                        }
                    }
                }
            }
            if (primaryKeyName == null)
                primaryKeyName = getRowKeyName();
        }

        return primaryKeyName;
    }
}
