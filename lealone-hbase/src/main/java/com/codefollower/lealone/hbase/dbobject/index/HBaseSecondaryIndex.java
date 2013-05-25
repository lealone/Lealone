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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
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
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.result.SortOrder;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueBytes;
import com.codefollower.lealone.value.ValueNull;

public class HBaseSecondaryIndex extends BaseIndex {

    final static byte[] PSEUDO_FAMILY = Bytes.toBytes("f");
    final static byte[] PSEUDO_COLUMN = Bytes.toBytes("c");

    private final static byte[] ZERO = { (byte) 0 };

    final HTable indexTable;
    private final int keyColumns;

    private ByteBuffer buffer = ByteBuffer.allocate(256);

    public HBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        keyColumns = columns.length + 1; //多加了一列，最后一列对应rowKey

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

    void printIndexTable() {
        //if (indexType.isUnique()) {
        System.out.println();
        try {
            ResultScanner resultScanner = indexTable.getScanner(new Scan());
            Result result = resultScanner.next();
            while (result != null) {
                System.out.println("Result " + toStringBinary(result.getRow()));
                result = resultScanner.next();
            }
            resultScanner.close();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        //}
    }

    public static String toStringBinary(final byte[] b) {
        if (b == null)
            return "null";
        return toStringBinary(b, 0, b.length);
    }

    public static String toStringBinary(final byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        try {
            String first = new String(b, off, len, "ISO-8859-1");
            for (int i = 0; i < first.length(); ++i) {
                int ch = first.charAt(i) & 0xFF;

                result.append(String.format("\\x%02X", ch));
            }
        } catch (UnsupportedEncodingException e) {
        }
        return result.toString();
    }

    @Override
    public void add(Session session, Row row) {
        if (indexType.isUnique()) {
            byte[] key = getKey(row);
            Result r;
            try {
                Scan scan = new Scan(key, (byte[]) null);
                scan.setCaching(1);
                scan.addColumn(PSEUDO_FAMILY, PSEUDO_COLUMN);
                ResultScanner resultScanner = indexTable.getScanner(scan);
                r = resultScanner.next();
                resultScanner.close();
            } catch (IOException e) {
                throw DbException.convert(e);
            }
            if (r != null && !r.isEmpty()) {
                //                System.out.println();
                //                System.out.println("Start   " + toStringBinary(key));
                //                System.out.println("ResultA " + toStringBinary(r.getRow()));
                //                System.out.println("ResultA " + Bytes.toStringBinary(r.getRow()));
                //                printIndexTable();

                buffer.clear();
                buffer.put(r.getRow());
                buffer.flip();
                SearchRow r2 = getRow(decode(buffer));
                if (compareRows(row, r2) == 0) {
                    if (!containsNullAndAllowMultipleNull(r2)) {
                        throw getDuplicateKeyException();
                    }
                }
            }
        }
        try {
            Put newPut = new Put(getKey(row));
            newPut.add(PSEUDO_FAMILY, PSEUDO_COLUMN, ZERO);
            indexTable.put(newPut);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    private byte[] getKey(SearchRow r) {
        if (r == null) {
            return null;
        }

        buffer.clear();
        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            array[i] = r.getValue(columns[i].getColumnId());
        }
        array[keyColumns - 1] = r.getRowKey();
        encode(buffer, array);
        return Bytes.toBytes(buffer);
    }

    //因为HBase在进行scan时查询的记录范围是startKey <= row < endKey(也就是不包含endKey)
    //而SQL是startKey <= row <= endKey
    //所以需要在原有的endKey上面多加一些额外的字节才会返回endKey
    private byte[] getLastKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        byte[] bytes;
        buffer.clear();
        Value[] array = new Value[columns.length];
        for (int i = 0; i < columns.length; i++) {
            array[i] = r.getValue(columns[i].getColumnId());
            if (array[i] == null || array[i] == ValueNull.INSTANCE) {
                buffer.putInt(Integer.MAX_VALUE); //lastKey查询不用0，而是用最大值
            } else {
                bytes = HBaseUtils.toBytes(array[i]);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
            }
        }
        buffer.putInt(Integer.MAX_VALUE);
        buffer.flip();
        System.out.println("lastKey " + toStringBinary(Bytes.toBytes(buffer)));
        printIndexTable();
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

    public ByteBuffer encode(ByteBuffer buff, Value[] array) {
        byte[] bytes;
        for (int i = 0; i < array.length; i++) {
            Value v = array[i];
            if (v == null || v == ValueNull.INSTANCE) {
                buff.putInt(0);
            } else {
                bytes = HBaseUtils.toBytes(v);
                buff.putInt(bytes.length);
                buff.put(bytes);
            }
        }
        buff.flip();
        return buff;
    }

    public Value[] decode(ByteBuffer buff) {
        int length;
        Value[] array = new Value[keyColumns];

        for (int i = 0; i < columns.length; i++) {
            length = buff.getInt();
            if (length == 0) {
                array[i] = ValueNull.INSTANCE;
            } else {
                byte[] bytes = new byte[length];
                buff.get(bytes);
                array[i] = HBaseUtils.toValue(bytes, columns[i].getType());
            }
        }

        length = buff.getInt();
        if (length == 0) {
            array[keyColumns - 1] = ValueNull.INSTANCE;
        } else {
            byte[] bytes = new byte[length];
            buff.get(bytes);
            array[keyColumns - 1] = ValueBytes.getNoCopy(bytes);
        }
        return array;
    }

    @Override
    public void remove(Session session, Row row) {
        if (((HBaseRow) row).isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            Delete delete;
            byte[] key = getKey(row);
            if (row.getStartTimestamp() != null)
                delete = new Delete(key, row.getStartTimestamp(), null);
            else
                delete = new Delete(key);
            indexTable.delete(delete);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        byte[] startRow = getKey(first);
        byte[] stopRow = getLastKey(last);
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
        HColumnDescriptor hcd = new HColumnDescriptor(PSEUDO_FAMILY);
        hcd.setMaxVersions(3);

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
