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
package com.codefollower.lealone.hbase.dbobject.index;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.index.BaseIndex;
import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.type.ValueDataType;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.result.SortOrder;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueArray;
import com.codefollower.lealone.value.ValueBytes;
import com.codefollower.lealone.value.ValueNull;

public class HBaseSecondaryIndex extends BaseIndex {

    final static byte[] FAMILY = Bytes.toBytes("f");
    final static byte[] ROWKEY = Bytes.toBytes("c");

    private final static byte[] ZERO = { (byte) 0 };

    final HTable indexTable;
    final ValueDataType keyType;
    private final int keyColumns;

    private ByteBuffer buffer = ByteBuffer.allocate(256);

    public HBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        keyColumns = columns.length + 1; //多加了一列，最后一列对应rowKey
        int[] sortTypes = new int[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            sortTypes[i] = columns[i].sortType;
        }
        sortTypes[keyColumns - 1] = SortOrder.ASCENDING;
        keyType = new ValueDataType(table.getDatabase().getCompareMode(), table.getDatabase(), sortTypes);
        try {
            indexTable = new HTable(HBaseUtils.getConfiguration(), indexName);
        } catch (IOException e) {
            throw DbException.convert(e);
        }

    }

    private static void checkIndexColumnTypes(IndexColumn[] columns) {
        for (IndexColumn c : columns) {
            int type = c.column.getType();
            if (type == Value.CLOB || type == Value.BLOB) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                        "Index on BLOB or CLOB column: " + c.column.getCreateSQL());
            }
        }
    }

    @Override
    public void close(Session session) {
        try {
            indexTable.close();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void add(Session session, Row row) {
        try {
            Put oldPut = ((HBaseRow) row).getPut();

            if (indexType.isUnique()) {
                byte[] key = getKey(row, null);
                Result r;
                try {
                    r = indexTable.get(new Get(key));
                    Scan scan = new Scan(key, getKey(row, oldPut.getRow()));
                    scan.addColumn(HBaseSecondaryIndex.FAMILY, HBaseSecondaryIndex.ROWKEY);
                    ResultScanner resultScanner = indexTable.getScanner(scan);
                    r = resultScanner.next();
                    resultScanner.close();
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
                if (r != null && !r.isEmpty()) {
                    buffer.clear();
                    buffer.put(r.getRow());
                    buffer.flip();
                    ValueArray array = (ValueArray) keyType.read(buffer);

                    SearchRow r2 = getRow(array.getList());
                    if (compareRows(row, r2) == 0) {
                        if (!containsNullAndAllowMultipleNull(r2)) {
                            throw getDuplicateKeyException();
                        }
                    }
                }
            }

            Put newPut = new Put(getKey(row, oldPut.getRow()));
            newPut.add(FAMILY, ROWKEY, ZERO);
            indexTable.put(newPut);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    private byte[] getKey(SearchRow r, byte[] rowKey) {
        if (r == null) {
            return null;
        }

        buffer.clear();
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            array[i] = r.getValue(idx);
            if (array[i] == null)
                array[i] = ValueNull.INSTANCE;
        }
        if (rowKey != null)
            array[keyColumns - 1] = ValueBytes.getNoCopy(rowKey);
        else
            array[keyColumns - 1] = ValueNull.INSTANCE;
        buffer = keyType.write(buffer, ValueArray.get(array));
        buffer.limit(buffer.position());
        buffer.position(0);

        return Bytes.toBytes(buffer);
    }

    //因为HBase在进行scan时查询的记录范围是startKey <= row < endKey(也就是不包含endKey)
    //而SQL是startKey <= row <= endKey
    //所以需要在原有的endKey上面多加一些额外的字节才会返回endKey
    private byte[] getLastKey(SearchRow r, byte[] rowKey) {
        if (r == null) {
            return null;
        }

        buffer.clear();
        int keyColumns = this.keyColumns + 1;
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            int idx = c.getColumnId();
            array[i] = r.getValue(idx);
            if (array[i] == null)
                array[i] = ValueNull.INSTANCE;
        }
        if (rowKey != null)
            array[keyColumns - 2] = ValueBytes.getNoCopy(rowKey);
        else
            array[keyColumns - 2] = ValueNull.INSTANCE;
        array[keyColumns - 1] = ValueNull.INSTANCE;
        buffer = keyType.write(buffer, ValueArray.get(array));
        buffer.limit(buffer.position());
        buffer.position(0);

        return Bytes.toBytes(buffer);
    }

    SearchRow getRow(Value[] array) {
        SearchRow searchRow = getTable().getTemplateRow();
        searchRow.setRowKey((array[array.length - 1]));
        Column[] cols = getColumns();
        for (int i = 0, size = array.length - 1; i < size; i++) {
            Column c = cols[i];
            int idx = c.getColumnId();
            Value v = array[i];
            searchRow.setValue(idx, v);
        }
        return searchRow;
    }

    @Override
    public void remove(Session session, Row row) {
        if (((HBaseRow) row).isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            Delete delete;
            if (row.getStartTimestamp() != null)
                delete = new Delete(HBaseUtils.toBytes(row.getRowKey()), row.getStartTimestamp(), null);
            else {
                Result result = ((HBaseRow) row).getResult();
                delete = new Delete(HBaseUtils.toBytes(row.getRowKey()));
                if (result != null) {
                    for (KeyValue kv : result.list()) {
                        delete.deleteColumn(kv.getFamily(), kv.getQualifier(), kv.getTimestamp());
                    }
                }
            }
            indexTable.delete(delete);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        byte[] startRow = getKey(first, null);
        byte[] stopRow = getLastKey(last, null);
        return new HBaseSecondaryIndexCursor((HBaseTable) getTable(), this, startRow, stopRow);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        return find((TableFilter) null, first, last);
    }

    @Override
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return 10 * getCostRangeIndex(masks, 100, sortOrder);
    }

    @Override
    public void remove(Session session) {
        try {
            HBaseSecondaryIndex.dropIndexTableIfExists(getName());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void truncate(Session session) {
        try {
            HBaseSecondaryIndex.dropIndexTableIfExists(getName());
            HBaseSecondaryIndex.createIndexTableIfNotExists(getName());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public boolean canGetFirstOrLast() {
        return false;
    }

    @Override
    public Cursor findFirstOrLast(Session session, boolean first) {
        throw DbException.getUnsupportedException("findFirstOrLast");
    }

    @Override
    public boolean needRebuild() {
        return false;
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
    public void checkRename() {
    }

    @Override
    public long getDiskSpaceUsed() {
        return 0;
    }

    public synchronized static void createIndexTableIfNotExists(String indexName) throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
        hcd.setMaxVersions(1);

        HTableDescriptor htd = new HTableDescriptor(indexName);
        htd.addFamily(hcd);
        if (!admin.tableExists(indexName)) {
            admin.createTable(htd);
        }
    }

    public synchronized static void dropIndexTableIfExists(String indexName) throws Exception {
        HBaseAdmin admin = HBaseUtils.getHBaseAdmin();
        if (admin.tableExists(indexName)) {
            admin.disableTable(indexName);
            admin.deleteTable(indexName);
        }
    }
}
