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
import com.codefollower.lealone.expression.Expression;
import com.codefollower.lealone.hbase.command.dml.WithWhereClause;
import com.codefollower.lealone.hbase.dbobject.table.HBaseTable;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.result.HBaseRow;
import com.codefollower.lealone.hbase.transaction.Filter;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.result.Row;
import com.codefollower.lealone.result.SearchRow;
import com.codefollower.lealone.transaction.Transaction;
import com.codefollower.lealone.util.StringUtils;
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;

public class HBasePrimaryIndexCursor implements Cursor {
    private final HBaseSession session;
    private int fetchSize;
    private byte[] regionName = null;

    private long scannerId;
    private Result[] result;
    private int index = -1;
    private List<Column> columns;
    private byte[] defaultColumnFamilyName;
    private int columnCount;
    private String rowKeyName;
    private ResultInterface subqueryResult;
    private boolean isGet = false;

    public HBasePrimaryIndexCursor(TableFilter filter, SearchRow first, SearchRow last) {
        session = (HBaseSession) filter.getSession();
        Prepared p = filter.getPrepared();
        if (p instanceof WithWhereClause) {
            regionName = Bytes.toBytes(((WithWhereClause) p).getWhereClauseSupport().getRegionName());
        }

        if (regionName == null)
            throw DbException.convert(new NullPointerException("regionName is null"));

        fetchSize = filter.getPrepared().getFetchSize();

        rowKeyName = ((HBaseTable) filter.getTable()).getRowKeyName();
        columnCount = ((HBaseTable) filter.getTable()).getColumns().length;

        if (filter.getSelect() != null)
            columns = filter.getSelect().getColumns(filter);
        else
            columns = Arrays.asList(filter.getTable().getColumns());

        Value startValue = null;
        Value endValue = null;
        if (first != null)
            startValue = first.getRowKey();
        if (last != null)
            endValue = last.getRowKey();

        if (startValue != null && endValue != null && startValue == endValue) {
            try {
                isGet = true;
                Result r = session.getRegionServer().get(regionName, new Get(Bytes.toBytes(startValue.getString())));
                if (r != null && !r.isEmpty())
                    result = new Result[] { r };
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        } else if (filter.getSelect() != null && filter.getSelect().getTopTableFilter() != filter) {
            StringBuilder buff = new StringBuilder("SELECT * FROM ");
            buff.append(filter.getTable().getSQL());
            Expression filterCondition = filter.getFilterCondition();
            if (filterCondition != null) {
                buff.append(" WHERE ").append(StringUtils.unEnclose(filterCondition.getSQL()));
            }
            Prepared prepared = session.prepare(buff.toString(), true);
            columns = Arrays.asList(filter.getTable().getColumns());
            columnCount = columns.size();
            subqueryResult = prepared.query(-1);
        } else {
            byte[] startKey = HConstants.EMPTY_BYTE_ARRAY;
            byte[] endKey = HConstants.EMPTY_BYTE_ARRAY;

            Scan scan = new Scan();
            if (startValue != null)
                startKey = HBaseUtils.toBytes(startValue);
            if (endValue != null)
                endKey = HBaseUtils.toBytes(endValue);
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
            } catch (Exception e) {
                throw DbException.convert(e);
            }

            if (columns != null) {
                defaultColumnFamilyName = Bytes.toBytes(((HBaseTable) filter.getTable()).getDefaultColumnFamilyName());
                for (Column c : columns) {
                    if (rowKeyName.equalsIgnoreCase(c.getName()))
                        continue;
                    else if (c.getColumnFamilyName() != null)
                        scan.addColumn(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes());
                    else
                        scan.addColumn(defaultColumnFamilyName, c.getNameAsBytes());
                }
            }
            try {
                scannerId = session.getRegionServer().openScanner(regionName, scan);
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    @Override
    public Row get() {
        if (subqueryResult != null) {
            Value[] data = new Value[columnCount];
            Value[] data2 = subqueryResult.currentRow();

            Value rowKey = data2[0];
            if (columns != null) {
                int i = 0;
                for (Column c : columns) {
                    data[c.getColumnId()] = data2[i++];
                }
            }
            return new HBaseRow(regionName, rowKey, data, Row.MEMORY_CALCULATE);
        }
        if (result != null && index < result.length) {
            Result r = result[index];
            Value[] data = new Value[columnCount];
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
            return new HBaseRow(regionName, rowKey, data, Row.MEMORY_CALCULATE, r);
        }
        return null;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        index++;
        if (subqueryResult != null) {
            return subqueryResult.next();
        }
        if (result != null && index < result.length)
            return true;
        else if (isGet)
            return false;

        try {
            Transaction transaction = session.getTransaction();
            List<KeyValue> kvs;
            KeyValue kv;
            Result r;
            long queryTimestamp;
            result = session.getRegionServer().next(scannerId, fetchSize);
            ArrayList<Result> list = new ArrayList<Result>(result.length);
            try {
                for (int i = 0; i < result.length; i++) {
                    r = result[i];
                    kvs = r.list();
                    //当Result.isEmpty=true时，r.list()也返回null，所以这里不用再判断kvs.isEmpty
                    if (kvs != null) {
                        kv = kvs.get(0);
                        queryTimestamp = kv.getTimestamp();
                        if (queryTimestamp < transaction.getStartTimestamp() & queryTimestamp % 2 == 0) {
                            if (kv.getValueLength() != 0) //kv已删除，不需要再处理
                                list.add(r);
                            continue;
                        }
                    }

                    //TODO Filter.filter很慢
                    r = new Result(Filter.filter(session.getRegionServer(), regionName, transaction, kvs, 1));
                    if (!r.isEmpty())
                        list.add(r);
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }

            result = list.toArray(new Result[0]);

            index = 0;
        } catch (IOException e) {
            throw DbException.convert(e);
        }

        if (result != null && result.length > 0)
            return true;

        try {
            session.getRegionServer().close(scannerId);
        } catch (IOException e) {
            //ignore
        }
        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

}
