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
package org.lealone.hbase.dbobject.index;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.command.Prepared;
import org.lealone.dbobject.index.Cursor;
import org.lealone.dbobject.table.Column;
import org.lealone.dbobject.table.TableFilter;
import org.lealone.expression.Parameter;
import org.lealone.hbase.command.dml.WithWhereClause;
import org.lealone.hbase.dbobject.index.HBaseSecondaryIndex.Buffer;
import org.lealone.hbase.dbobject.table.HBaseTable;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.metadata.MetaDataAdmin;
import org.lealone.hbase.result.HBaseRow;
import org.lealone.hbase.transaction.ValidityChecker;
import org.lealone.hbase.util.HBaseRegionInfo;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;
import org.lealone.result.Row;
import org.lealone.result.SearchRow;
import org.lealone.util.New;
import org.lealone.value.Value;
import org.lealone.value.ValueString;

public class HBaseSecondaryIndexCursor implements Cursor {
    private final HBaseSecondaryIndex secondaryIndex;
    private final HBaseSession session;
    private final int fetchSize;
    private final byte[] regionName;

    private final byte[] defaultColumnFamilyName = MetaDataAdmin.DEFAULT_COLUMN_FAMILY;

    private final long scannerId;
    private final List<Column> columns;

    private Result[] result;
    private int index = -1;

    private SearchRow searchRow;
    private HBaseRow row;

    public HBaseSecondaryIndexCursor(HBaseSecondaryIndex index, TableFilter filter, byte[] startKey, byte[] endKey) {
        secondaryIndex = index;
        session = (HBaseSession) filter.getSession();
        HRegionServer rs = session.getRegionServer();

        Prepared p = filter.getPrepared();
        if (!(p instanceof WithWhereClause))
            throw DbException.throwInternalError("not instanceof WithWhereClause: " + p);

        regionName = Bytes.toBytes(((WithWhereClause) p).getWhereClauseSupport().getRegionName());
        if (regionName == null)
            throw DbException.throwInternalError("regionName is null");

        fetchSize = p.getFetchSize();

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

            scannerId = rs.openScanner(regionName, scan);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private byte[] dataTableName;
    private Prepared selectPrepared;
    private Parameter selectParameter;

    private void initSelectPrepared() {
        StringBuilder buff = new StringBuilder("SELECT ");

        if (columns != null) {
            int i = 0;
            for (Column c : columns) {
                if (i > 0)
                    buff.append(',');
                buff.append(c.getFullName());
                i++;
            }
        } else {
            buff.append("*");
        }
        buff.append(" FROM ");
        buff.append(secondaryIndex.getTable().getSQL());

        HBaseTable htable = (HBaseTable) secondaryIndex.getTable();
        buff.append(" WHERE ").append(htable.getPrimaryKeyName()).append("=?");
        selectPrepared = session.prepare(buff.toString(), true);
        selectParameter = selectPrepared.getParameters().get(0);
        dataTableName = htable.getTableNameAsBytes();
    }

    @Override
    public Row get() {
        if (row == null) {
            if (searchRow != null) {
                if (selectPrepared == null)
                    initSelectPrepared();

                byte[] rowKey = HBaseUtils.toBytes(searchRow.getRowKey());
                HBaseRegionInfo regionInfo = HBaseUtils.getHBaseRegionInfo(dataTableName, rowKey);
                selectParameter.setValue(ValueString.get(Bytes.toString(rowKey)));
                ResultInterface r = selectPrepared.query(1);
                if (r.next()) {
                    Value[] data = r.currentRow();
                    List<Column> cols = columns;
                    if (cols == null)
                        cols = Arrays.asList(secondaryIndex.getTable().getColumns());

                    List<KeyValue> kvs = New.arrayList(cols.size());
                    for (Column c : columns) {
                        kvs.add(new KeyValue(rowKey, c.getColumnFamilyNameAsBytes(), c.getNameAsBytes()));
                    }
                    row = new HBaseRow(regionInfo.getRegionNameAsBytes(), searchRow.getRowKey(), data, Row.MEMORY_CALCULATE,
                            new Result(kvs));
                } else {
                    throw new RuntimeException("row key " + searchRow.getRowKey() + " not found");
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
        Buffer buffer = new Buffer(result[index].getRow());
        searchRow = secondaryIndex.getRow(buffer);
    }

    @Override
    public boolean next() {
        searchRow = null;
        row = null;
        index++;
        if (result != null && index < result.length) {
            setSearchRow();
            return true;
        }

        try {
            result = ValidityChecker.fetchResults(defaultColumnFamilyName, session, regionName, scannerId, fetchSize);
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
