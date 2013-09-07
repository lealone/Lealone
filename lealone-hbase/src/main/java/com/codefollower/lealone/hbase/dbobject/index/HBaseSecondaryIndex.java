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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.index.BaseIndex;
import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.index.IndexType;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.IndexColumn;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.metadata.MetaDataAdmin;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.result.SortOrder;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueBytes;
import com.codefollower.lealone.value.ValueNull;
import com.codefollower.lealone.value.ValueString;

public class HBaseSecondaryIndex extends BaseIndex {

    public synchronized static void createIndexTableIfNotExists(Session session, String indexName) {
        StringBuilder buff = new StringBuilder("CREATE HBASE TABLE IF NOT EXISTS ");
        buff.append(indexName).append(" (COLUMN FAMILY ").append(Bytes.toString(MetaDataAdmin.DEFAULT_COLUMN_FAMILY));
        buff.append("(C char))");

        Prepared p = session.prepare(buff.toString(), true);
        p.setExecuteDirec(true);
        p.update();
    }

    public synchronized static void dropIndexTableIfExists(Session session, String indexName) {
        Prepared p = session.prepare("DROP TABLE IF EXISTS " + indexName, true);
        p.setExecuteDirec(true);
        p.update();
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

    //组成索引key的列个数
    private final int keyColumns;
    private final byte[] indexTableNameAsBytes;

    private final String select;
    private final String insert;
    private final String delete;

    public HBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        if (!database.isStarting()) {
            checkIndexColumnTypes(columns);
        }
        keyColumns = columns.length + 1; //多加了一列，最后一列对应rowKey
        indexTableNameAsBytes = Bytes.toBytes(indexName);

        select = "select _rowkey_ from " + indexName + " where _rowkey_>=?";
        insert = "insert into " + indexName + "(_rowkey_, c) values(?,'0')";
        delete = "delete from " + indexName + " where _rowkey_=?";
    }

    public byte[] getTableNameAsBytes() {
        return indexTableNameAsBytes;
    }

    @Override
    public void add(Session session, Row row) {
        if (indexType.isUnique()) {
            byte[] key = getStartKey(row);
            Prepared p = session.prepare(select, true);
            p.getParameters().get(0).setValue(ValueString.get(Bytes.toString(key)));
            ResultInterface r = p.query(1);
            try {
                if (r.next()) {
                    SearchRow r2 = getRow(new Buffer(Bytes.toBytes(r.currentRow()[0].getString())));
                    if (compareRows(row, r2) == 0) {
                        if (!containsNullAndAllowMultipleNull(r2)) {
                            throw getDuplicateKeyException();
                        }
                    }
                }
            } finally {
                r.close();
            }
        }

        Prepared p = session.prepare(insert, true);
        p.getParameters().get(0).setValue(ValueString.get(Bytes.toString(getKey(row))));
        p.update();
    }

    //参数row是主表的记录，并不是索引表的记录
    @Override
    public void remove(Session session, Row row) {
        //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
        if (((HBaseRow) row).isForUpdate())
            return;

        //删除操作转成insert null操作
        Prepared p = session.prepare(delete, true);
        p.getParameters().get(0).setValue(ValueString.get(Bytes.toString(getKey(row))));
        p.update();
    }

    @Override
    public Cursor find(TableFilter filter, SearchRow first, SearchRow last) {
        byte[] startRow = getKey(first);
        byte[] stopRow = getLastKey(last);
        return new HBaseSecondaryIndexCursor(this, filter, startRow, stopRow);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        throw DbException.getUnsupportedException("find(Session, SearchRow, SearchRow)");
    }

    @Override
    public void close(Session session) {
        // nothing to do
    }

    public byte[] getKey(SearchRow r) {
        return getKey(r, false);
    }

    private byte[] getKey(SearchRow r, boolean isStartKey) {
        if (r == null) {
            return null;
        }

        Value[] array = new Value[keyColumns];
        for (int i = 0; i < columns.length; i++) {
            array[i] = r.getValue(columns[i].getColumnId());
        }
        if (isStartKey)
            array[keyColumns - 1] = null;
        else
            array[keyColumns - 1] = r.getRowKey();

        try {
            return encode(array);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    //用于检查唯一约束是否违反
    private byte[] getStartKey(SearchRow r) {
        return getKey(r, true);
    }

    //因为HBase在进行scan时查询的记录范围是startKey <= row < endKey(也就是不包含endKey)
    //而SQL是startKey <= row <= endKey
    //所以需要在原有的endKey上面多加一些额外的字节才会返回endKey
    private byte[] getLastKey(SearchRow r) {
        if (r == null) {
            return null;
        }
        byte[] bytes;
        Buffer buffer = BufferPool.getBuffer();
        try {
            Value[] array = new Value[columns.length];
            for (int i = 0; i < columns.length; i++) {
                array[i] = r.getValue(columns[i].getColumnId());
                if (array[i] == null || array[i] == ValueNull.INSTANCE) {
                    buffer.writeInt(Integer.MAX_VALUE); //lastKey查询不用0，而是用最大值
                } else {
                    bytes = HBaseUtils.toBytes(array[i]);
                    buffer.writeInt(bytes.length);
                    buffer.write(bytes);
                }
            }
            buffer.writeInt(Integer.MAX_VALUE);
            bytes = buffer.toByteArray();
            return bytes;
        } catch (IOException e) {
            throw DbException.convert(e);
        } finally {
            BufferPool.pushBuffer(buffer);
        }
    }

    SearchRow getRow(Buffer buffer) {
        try {
            return getRow(decode(buffer));
        } catch (IOException e) {
            throw DbException.convert(e);
        }
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

    private byte[] encode(Value[] array) throws IOException {
        Buffer buffer = BufferPool.getBuffer();
        try {
            byte[] bytes;
            for (int i = 0; i < array.length; i++) {
                Value v = array[i];
                if (v == null || v == ValueNull.INSTANCE) {
                    buffer.writeInt(0);
                } else {
                    bytes = HBaseUtils.toBytes(v);
                    buffer.writeInt(bytes.length);
                    buffer.write(bytes);
                }
            }
            buffer.writeInt(Integer.MAX_VALUE);
            bytes = buffer.toByteArray();
            return bytes;
        } catch (IOException e) {
            throw DbException.convert(e);
        } finally {
            BufferPool.pushBuffer(buffer);
        }
    }

    private Value[] decode(Buffer buff) throws IOException {
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
    public double getCost(Session session, int[] masks, SortOrder sortOrder) {
        return 10 * getCostRangeIndex(masks, 100, sortOrder);
    }

    @Override
    public void remove(Session session) {
        try {
            HBaseSecondaryIndex.dropIndexTableIfExists(session, getName());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void truncate(Session session) {
        try {
            HBaseSecondaryIndex.dropIndexTableIfExists(session, getName());
            HBaseSecondaryIndex.createIndexTableIfNotExists(session, getName());
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

    private static class BufferPool {
        private static BlockingQueue<Buffer> pool = new LinkedBlockingQueue<Buffer>();

        public static Buffer getBuffer() {
            Buffer buffer = pool.poll();
            if (buffer != null) {
                buffer.reset();
                return buffer;
            }
            return new Buffer();
        }

        public static void pushBuffer(Buffer buffer) {
            pool.add(buffer);
        }
    }

    public static class Buffer extends DataOutputStream {
        private ByteArrayOutputStream baos;
        private DataInputStream in;

        public Buffer() {
            super(new ByteArrayOutputStream(256));
            baos = (ByteArrayOutputStream) this.out;
        }

        public Buffer(byte[] bytes) {
            super(null);
            in = new DataInputStream(new ByteArrayInputStream(bytes));
        }

        public byte[] toByteArray() {
            return baos.toByteArray();
        }

        public void reset() {
            baos.reset();
        }

        public void get(byte[] b) throws IOException {
            in.read(b);
        }

        public int getInt() throws IOException {
            return in.readInt();
        }
    }
}
