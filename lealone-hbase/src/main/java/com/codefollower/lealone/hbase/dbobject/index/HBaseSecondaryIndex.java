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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

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
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;

public class HBaseSecondaryIndex extends BaseIndex {

    private final static byte[] FAMILY = Bytes.toBytes("cf");
    private final static byte[] ROWKEY = Bytes.toBytes("rk");

    private final HTable indexTable;

    public HBaseSecondaryIndex(Table table, int id, String indexName, IndexColumn[] columns, IndexType indexType) {
        initBaseIndex(table, id, indexName, columns, indexType);
        try {
            indexTable = new HTable(HBaseUtils.getConfiguration(), indexName);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void close(Session session) {
    }

    @Override
    public void add(Session session, Row row) {
        try {
            Put oldPut = ((HBaseRow) row).getPut();
            Put newPut = new Put(getIndexRowKey(row));
            newPut.add(FAMILY, ROWKEY, oldPut.getTimeStamp(), oldPut.getRow());
            indexTable.put(newPut);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    private byte[] getIndexRowKey(Row row) {
        if (columns.length == 1)
            return HBaseUtils.toBytes(row.getValue(columns[0].getColumnId()));
        StringBuilder buff = new StringBuilder();
        for (Column c : columns) {
            if (buff.length() > 0)
                buff.append("_");
            int idx = c.getColumnId();
            buff.append(row.getValue(idx).getString());
        }
        return HBaseUtils.toBytes(buff.toString());
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
        return new HBaseSecondaryIndexCursor(filter, first, last);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
        throw DbException.getUnsupportedException("find(Session, SearchRow, SearchRow)");
    }

    @Override
    public double getCost(Session session, int[] masks) {
        return 0;
    }

    @Override
    public void remove(Session session) {
    }

    @Override
    public void truncate(Session session) {
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
