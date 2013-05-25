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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;

public class HBaseSecondaryIndexCursor implements Cursor {
    private final ByteBuffer readBuffer = ByteBuffer.allocate(256);
    private final HTable dataTable;
    private final HBaseSecondaryIndex index;
    private final ResultScanner resultScanner;

    private final Column[] columns;

    private SearchRow searchRow;
    private Row row;

    public HBaseSecondaryIndexCursor(HBaseTable hbaseTable, HBaseSecondaryIndex index, byte[] startRow, byte[] stopRow) {
        this.index = index;
        this.columns = hbaseTable.getColumns();

        Scan scan = new Scan(startRow, stopRow);
        scan.addColumn(HBaseSecondaryIndex.PSEUDO_FAMILY, HBaseSecondaryIndex.PSEUDO_COLUMN);
        try {
            resultScanner = index.indexTable.getScanner(scan);
            dataTable = new HTable(HBaseUtils.getConfiguration(), index.getTable().getName());
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    private void close() {
        try {
            dataTable.close();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
        resultScanner.close();
    }

    @Override
    public Row get() {
        if (row == null) {
            if (searchRow != null) {
                Result r;
                try {
                    r = dataTable.get(new Get(HBaseUtils.toBytes(searchRow.getRowKey())));
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
                if (r != null) {
                    Value[] data = new Value[columns.length];
                    Value rowKey = ValueString.get(Bytes.toString(r.getRow()));
                    if (columns != null) {
                        int i = 0;
                        for (Column c : columns) {
                            i = c.getColumnId();
                            if (c.isRowKeyColumn())
                                data[i] = rowKey;
                            else
                                data[i] = HBaseUtils.toValue( //
                                        r.getValue(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes()), c.getType());
                        }
                    }
                    row = new HBaseRow(null, rowKey, data, Row.MEMORY_CALCULATE, r);
                }
            }
        }
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return searchRow;
    }

    @Override
    public boolean next() {
        readBuffer.clear();
        searchRow = null;
        row = null;
        Result result;
        try {
            result = resultScanner.next();
        } catch (IOException e) {
            close();
            throw DbException.convert(e);
        }
        if (result == null) {
            close();
            return false;
        }

        readBuffer.put(result.getRow());
        readBuffer.flip();
        searchRow = index.getRow(index.decode(readBuffer));

        return true;
    }

    @Override
    public boolean previous() {
        return false;
    }

}
