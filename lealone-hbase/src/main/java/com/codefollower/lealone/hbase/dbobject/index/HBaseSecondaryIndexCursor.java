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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.dbobject.index.Cursor;
import com.codefollower.lealone.dbobject.table.Column;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.hbase.command.dml.WithWhereClause;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.transaction.ValidityChecker;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.transaction.Transaction;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;

public class HBaseSecondaryIndexCursor implements Cursor {
    private final byte[] defaultColumnFamilyName;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(256);
    private final HBaseSecondaryIndex secondaryIndex;
    private final HBaseSession session;
    private final String hostAndPort;
    private int fetchSize;
    private byte[] regionName = null;

    private long scannerId;
    private Result[] result;
    private int index = -1;
    private List<Column> columns;
    private SearchRow searchRow;
    private long searchTimestamp;
    private Row row;

    public HBaseSecondaryIndexCursor(HBaseSecondaryIndex index, TableFilter filter, byte[] startKey, byte[] endKey) {
        defaultColumnFamilyName = Bytes.toBytes(((HBaseTable) filter.getTable()).getDefaultColumnFamilyName());
        secondaryIndex = index;
        session = (HBaseSession) filter.getSession();
        hostAndPort = session.getRegionServer().getServerName().getHostAndPort();
        Prepared p = filter.getPrepared();
        if (p instanceof WithWhereClause) {
            regionName = Bytes.toBytes(((WithWhereClause) p).getWhereClauseSupport().getRegionName());
        }

        if (regionName == null)
            throw DbException.convert(new NullPointerException("regionName is null"));

        fetchSize = filter.getPrepared().getFetchSize();

        if (filter.getSelect() != null)
            columns = filter.getSelect().getColumns(filter);
        else
            columns = Arrays.asList(filter.getTable().getColumns());

        if (startKey == null)
            startKey = HConstants.EMPTY_BYTE_ARRAY;
        if (endKey == null)
            endKey = HConstants.EMPTY_BYTE_ARRAY;

        Scan scan = new Scan();
        scan.setMaxVersions(1);
        try {
            HRegionInfo info = session.getRegionServer().getRegionInfo(regionName);
            if (Bytes.compareTo(startKey, info.getStartKey()) >= 0)
                scan.setStartRow(startKey);
            else
                scan.setStartRow(info.getStartKey());

            if (Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY))
                scan.setStopRow(info.getEndKey());
            else if (Bytes.compareTo(endKey, info.getEndKey()) < 0)
                scan.setStopRow(endKey);
            else
                scan.setStopRow(info.getEndKey());

            scan.addColumn(HBaseSecondaryIndex.PSEUDO_FAMILY, HBaseSecondaryIndex.PSEUDO_COLUMN);

            scannerId = session.getRegionServer().openScanner(regionName, scan);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Row get() {
        if (row == null) {
            if (searchRow != null) {
                Result r;
                try {
                    Get get = new Get(HBaseUtils.toBytes(searchRow.getRowKey()));
                    get.setTimeStamp(searchTimestamp);
                    if (columns != null) {
                        for (Column c : columns) {
                            if (c.isRowKeyColumn())
                                continue;
                            else if (c.getColumnFamilyName() != null)
                                get.addColumn(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes());
                            else
                                get.addColumn(defaultColumnFamilyName, c.getNameAsBytes());
                        }
                    }
                    r = secondaryIndex.dataTable.get(get);
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
                if (r != null) {
                    Value[] data = new Value[columns.size()];
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

    private void setSearchRow() {
        readBuffer.put(result[index].getRow());
        readBuffer.flip();
        searchRow = secondaryIndex.getRow(secondaryIndex.decode(readBuffer));
        searchTimestamp = result[index].raw()[0].getTimestamp();
    }

    @Override
    public boolean next() {
        readBuffer.clear();
        searchRow = null;
        row = null;
        index++;
        if (result != null && index < result.length) {
            setSearchRow();
            return true;
        }

        Transaction transaction = session.getTransaction();
        List<KeyValue> kvs;
        KeyValue kv;
        Result r;
        long queryTimestamp;
        try {
            result = session.getRegionServer().next(scannerId, fetchSize);
            ArrayList<Result> list = new ArrayList<Result>(result.length);
            for (int i = 0; i < result.length; i++) {
                r = result[i];
                kvs = r.list();
                //当Result.isEmpty=true时，r.list()也返回null，所以这里不用再判断kvs.isEmpty
                if (kvs != null) {
                    kv = kvs.get(0);
                    queryTimestamp = kv.getTimestamp();
                    if (queryTimestamp < transaction.getStartTimestamp() && queryTimestamp % 2 == 0) {
                        if (kv.getValueLength() != 0) //kv已删除，不需要再处理
                            list.add(r);
                        continue;
                    }
                }

                r = new Result(ValidityChecker.check(session.getRegionServer(), hostAndPort, regionName, transaction, kvs, 1));
                if (!r.isEmpty())
                    list.add(r);
            }

            result = list.toArray(new Result[0]);
        } catch (Exception e) {
            close();
            throw DbException.convert(e);
        }

        index = 0;

        if (result != null && result.length > 0) {
            setSearchRow();
            return true;
        }

        close();
        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

    private void close() {
        try {
            session.getRegionServer().close(scannerId);
        } catch (IOException e) {
            //ignore
        }
    }
}
