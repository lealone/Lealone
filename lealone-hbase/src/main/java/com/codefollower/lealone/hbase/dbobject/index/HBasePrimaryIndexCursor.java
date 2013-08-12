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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
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
import com.codefollower.lealone.value.Value;
import com.codefollower.lealone.value.ValueString;

//执行select、delete、update语句都会触发此类
public class HBasePrimaryIndexCursor implements Cursor {
    private final HBaseSession session;
    private final String hostAndPort;
    private final byte[] regionName;
    private final int fetchSize;

    private final byte[] defaultColumnFamilyName;

    //表所有列的个数
    private final int columnCount;
    //所要查询的列，不一定是表中的所有列，所以columnCount >= columns.size()
    private final List<Column> columns;

    private final boolean isGet;
    private long scannerId;

    private Result[] result;
    private int index = -1;

    private InternalScanner scanner;
    private boolean isEnd = false;
    private ArrayList<Result> tmpList;

    /**
     * 
     * @param filter 表过滤器
     * @param first 所有查找的主键列的开始值
     * @param last 所有查找的主键列的结束值
     */
    public HBasePrimaryIndexCursor(TableFilter filter, SearchRow first, SearchRow last) {
        session = (HBaseSession) filter.getSession();
        HRegionServer rs = session.getRegionServer();
        //getHostAndPort()是一个字符串拼接操作，这里是一个小优化，避免在next()方法中重复调用
        hostAndPort = rs.getServerName().getHostAndPort();

        Prepared p = filter.getPrepared();
        if (!(p instanceof WithWhereClause))
            throw DbException.throwInternalError("not instanceof WithWhereClause: " + p);

        regionName = Bytes.toBytes(((WithWhereClause) p).getWhereClauseSupport().getRegionName());
        if (regionName == null)
            throw DbException.throwInternalError("regionName is null");

        fetchSize = p.getFetchSize();
        defaultColumnFamilyName = ((HBaseTable) filter.getTable()).getDefaultColumnFamilyNameAsBytes();
        columnCount = filter.getTable().getColumns().length;

        //select语句
        //对于下面两种类型的sql，columns会是null
        //1: select count(*) from t (不带where条件)
        //2: select _rowkey_ from t
        if (filter.getSelect() != null)
            columns = filter.getSelect().getColumns(filter);
        else
            columns = Arrays.asList(filter.getTable().getColumns()); //delete、update语句

        Value startValue = null;
        Value endValue = null;
        if (first != null)
            startValue = first.getRowKey();
        if (last != null)
            endValue = last.getRowKey();

        //优化where pk = xxx，对于这样的等号查询，startValue和endValue相等，直接使用get方式获取数据
        if (startValue != null && endValue != null && startValue == endValue) {
            try {
                Result r = rs.get(regionName, new Get(Bytes.toBytes(startValue.getString())));
                r = ValidityChecker.checkResult(defaultColumnFamilyName, session, rs, hostAndPort, regionName,
                        session.getTransaction(), r);
                if (r != null)
                    result = new Result[] { r };

                isGet = true;
                scannerId = -1;
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        } else {
            isGet = false;

            Scan scan = new Scan();
            scan.setMaxVersions(1); //只取一个版本

            byte[] startKey = HConstants.EMPTY_BYTE_ARRAY;
            byte[] endKey = HConstants.EMPTY_BYTE_ARRAY;

            if (startValue != null)
                startKey = HBaseUtils.toBytes(startValue);
            if (endValue != null)
                endKey = HBaseUtils.toBytes(endValue);

            //调整start和stop位置，不能直接使用原有的startValue和endValue，因为它们有可能不是正确的Region开始和结束范围
            try {
                HRegionInfo info = rs.getRegionInfo(regionName);
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
                for (Column c : columns) {
                    if (!c.isRowKeyColumn()) {
                        //只指定列族而不指定具体的列会得到更好的性能
                        //scan.addColumn(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes());
                        scan.addFamily(c.getColumnFamilyNameAsBytes());
                    }
                }
            }

            scan.addFamily(defaultColumnFamilyName);

            //对于聚合运算，直接使用InternalScanner性能会更好，也无需启用ScannerListener，
            //因为聚合运算完成后InternalScanner就会被主动关闭，
            //通过scannerId的方式可能延迟到client端读取所有结果后才关闭或直到超时为止。
            if (filter.getSelect() != null && filter.getSelect().isGroupQuery()) {
                tmpList = new ArrayList<Result>(fetchSize);
                try {
                    //TODO 这里绕过HRegionServer的检查流程了
                    //如何像HRegionServer.execCoprocessor那样走正常的流程
                    scanner = rs.getOnlineRegion(regionName).getScanner(scan);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            } else {
                try {
                    scannerId = rs.openScanner(regionName, scan);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }
    }

    @Override
    public Row get() {
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
        if (result != null && index < result.length)
            return true;
        else if (isGet)
            return false;

        if (isEnd)
            return false;

        try {
            if (scanner != null) {
                tmpList.clear();
                isEnd = !ValidityChecker.fetchResults(defaultColumnFamilyName, session, hostAndPort, regionName, scanner,
                        fetchSize, tmpList);
                result = tmpList.toArray(new Result[tmpList.size()]);
                if (isEnd)
                    close();
            } else
                result = ValidityChecker.fetchResults(defaultColumnFamilyName, session, hostAndPort, regionName, scannerId,
                        fetchSize);
        } catch (Exception e) {
            close();
            throw DbException.convert(e);
        }

        index = 0;

        if (result != null && result.length > 0)
            return true;

        close();
        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

    private void close() {
        try {
            if (scanner != null)
                scanner.close();
            else
                session.getRegionServer().close(scannerId);
        } catch (IOException e) {
            //ignore
        }
    }
}
