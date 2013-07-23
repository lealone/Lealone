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
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

    final static byte[] PSEUDO_FAMILY = Bytes.toBytes("F");
    final static byte[] PSEUDO_COLUMN = Bytes.toBytes("C");

    private final static byte[] ZERO = { (byte) 0 };

    final HTable indexTable;
    final HTable dataTable;
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
            dataTable = new HTable(HBaseUtils.getConfiguration(), table.getName());
        } catch (IOException e) {
            throw DbException.convert(e);
        }

    }

    public byte[] getTableNameAsBytes() {
        return indexTable.getTableName();
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

    public byte[] getKey(SearchRow r) {
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
    public void add(Session session, Row row) {
        if (indexType.isUnique()) {
            byte[] key = getKey(row);
            Prepared p = session.prepare("select _rowkey_ from " + getName() + " where _rowkey_>=?", true);
            p.getParameters().get(0).setValue(ValueString.get(Bytes.toString(key)));
            ResultInterface r = p.query(1);
            if (r.next()) {
                buffer.clear();
                buffer.put(Bytes.toBytes(r.currentRow()[0].getString()));
                buffer.flip();
                r.close();
                SearchRow r2 = getRow(decode(buffer));
                if (compareRows(row, r2) == 0) {
                    if (!containsNullAndAllowMultipleNull(r2)) {
                        throw getDuplicateKeyException();
                    }
                }
            }
        }
        try {
            //分两种场景:
            //1. 以insert into这类SQL语句插入记录
            //   这种场景用正常的Put操作
            //
            //2. 以delete from这类SQL语句删除记录时出错了
            //   这种方式实际上并不真的删除原有记录，而是插入一条值为null的新版本记录，出错时撤消，
            //   所以要用Delete
            if (((HBaseRow) row).getResult() == null) {
                Put newPut = new Put(getKey(row));
                newPut.add(PSEUDO_FAMILY, PSEUDO_COLUMN, row.getTransactionId(), ZERO);
                indexTable.put(newPut);
            } else {
                Delete delete = new Delete(getKey(row));
                delete.deleteColumn(PSEUDO_FAMILY, PSEUDO_COLUMN, row.getTransactionId());
                indexTable.delete(delete);
            }
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void remove(Session session, Row row) { //参数row是主表的记录，并不是索引表的记录
        if (((HBaseRow) row).isForUpdate()) //Update这种类型的SQL不需要先删除再insert，只需直接insert即可
            return;
        try {
            //分两种场景:
            //1. 以delete from这类SQL语句删除记录
            //   这种场景会把要删除的记录找出来，此时用put的方式，不能直接用Delete，因为用Delete后如果当前事务未提交
            //   那么其它并发事务就找不到之前的记录版本
            //
            //2. 在进行insert时出错了
            //   比如此索引后面有一个唯一索引，往唯一索引insert了重复值，那么就出错，此时立即撤消，
            //   因为是新记录，所以要用Delete
            if (((HBaseRow) row).getResult() != null) {
                Put put = new Put(getKey(row));
                put.add(PSEUDO_FAMILY, PSEUDO_COLUMN, row.getTransactionId(), null);
                indexTable.put(put);
            } else {
                Delete delete = new Delete(getKey(row));
                delete.deleteColumn(PSEUDO_FAMILY, PSEUDO_COLUMN, row.getTransactionId());
                indexTable.delete(delete);
            }
        } catch (IOException e) {
            throw DbException.convert(e);
        }
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

    public synchronized static void createIndexTableIfNotExists(Session session, String indexName) throws Exception {
        Prepared p = session.prepare("CREATE HBASE TABLE IF NOT EXISTS " + indexName + " (COLUMN FAMILY f(c char))", true);
        p.setExecuteDirec(true);
        p.update();
    }

    public synchronized static void dropIndexTableIfExists(Session session, String indexName) throws Exception {
        Prepared p = session.prepare("DROP TABLE IF EXISTS " + indexName, true);
        p.setExecuteDirec(true);
        p.update();
    }
}
