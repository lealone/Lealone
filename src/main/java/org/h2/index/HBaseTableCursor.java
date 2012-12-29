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
package org.h2.index;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.engine.Session;
import org.h2.result.CombinedResult;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.table.Column;
import org.h2.table.HBaseTable;
import org.h2.table.TableFilter;
import org.h2.util.HBaseUtils;
import org.h2.value.Value;
import org.h2.value.ValueString;

public class HBaseTableCursor implements Cursor {
    private final Session session;
    private long scannerId;
    private Result[] result;
    private int length = 0;
    private List<Column> columns;
    private byte[] defaultColumnFamilyName;
    private int columnCount = 0;
    private String rowKeyName;
    private CombinedResult combinedResult;

    public HBaseTableCursor(TableFilter filter, SearchRow first, SearchRow last) {
        this.session = filter.getSession();
        rowKeyName = ((HBaseTable) filter.getTable()).getRowKeyName();
        columnCount = ((HBaseTable) filter.getTable()).getColumns().length;
        columns = filter.getSelect().getColumns(filter);
        if (filter.getSelect().getTopTableFilter() != filter) {
            combinedResult = new CombinedResult(filter);
        } else {
            byte[] startRowKey = null;
            byte[] stopRowKey = null;
            String[] rowKeys = filter.getSelect().getRowKeys();
            if (rowKeys != null) {
                if (rowKeys.length >= 1 && rowKeys[0] != null)
                    startRowKey = Bytes.toBytes(rowKeys[0]);

                if (rowKeys.length >= 2 && rowKeys[1] != null)
                    stopRowKey = Bytes.toBytes(rowKeys[1]);
            }

            if (startRowKey == null)
                startRowKey = HConstants.EMPTY_START_ROW;
            if (stopRowKey == null)
                stopRowKey = HConstants.EMPTY_END_ROW;

            Scan scan = new Scan();
            if (startRowKey != null)
                scan.setStartRow(startRowKey);
            if (stopRowKey != null)
                scan.setStopRow(stopRowKey);

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
                byte[] regionName = filter.getSelect().getRegionName();
                if (regionName == null)
                    regionName = session.getRegionName();
                scannerId = session.getRegionServer().openScanner(regionName, scan);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public HBaseTableCursor(Session session, SearchRow first, SearchRow last) {
        this.session = session;
        try {
            Scan scan = new Scan();
            if (first != null)
                scan.setStartRow(Bytes.toBytes(Long.toString(first.getKey())));
            if (last != null)
                scan.setStopRow(Bytes.toBytes(Long.toString(last.getKey())));
            scannerId = session.getRegionServer().openScanner(session.getRegionName(), scan);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Row get() {
        if (combinedResult != null) {
            Value[] data = new Value[columnCount];
            Value[] data2 = combinedResult.currentRow();

            Value rowKey = data2[0];
            if (columns != null) {
                int i = 1;
                for (Column c : columns) {
                    data[c.getColumnId()] = data2[i++];
                }
            }
            return new Row(rowKey, data, Row.MEMORY_CALCULATE);
        }
        if (result != null && length < result.length) {
            Result r = result[length++];

            Value[] data = new Value[columnCount];
            Value rowKey = ValueString.get(Bytes.toString(r.getRow()));
            if (columns != null) {
                int i = 0;
                for (Column c : columns) {
                    i = c.getColumnId();
                    if (rowKeyName.equalsIgnoreCase(c.getName()))
                        ;//rowKey = ValueString.get(Bytes.toString(r.getRow()));
                    else
                        data[i] = HBaseUtils.toValue(r.getValue(c.getColumnFamilyNameAsBytes(), c.getNameAsBytes()), c.getType());
                }
            }
            return new Row(rowKey, data, Row.MEMORY_CALCULATE);
        }
        return null;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        if (combinedResult != null) {
            return combinedResult.next();
        }
        if (result != null && length < result.length)
            return true;

        try {
            result = session.getRegionServer().next(scannerId, session.getFetchSize());
            length = 0;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (result != null && result.length > 0)
            return true;

        return false;
    }

    @Override
    public boolean previous() {
        return false;
    }

}
