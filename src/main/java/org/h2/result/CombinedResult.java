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
package org.h2.result;

import java.util.List;
import java.util.Properties;

import org.h2.command.dml.Query;
import org.h2.engine.Session;
import org.h2.expression.RowKeyConditionInfo;
import org.h2.jdbc.JdbcConnection;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.util.HBaseRegionInfo;
import org.h2.util.HBaseUtils;
import org.h2.util.JdbcUtils;
import org.h2.util.ValueHashMap;
import org.h2.value.Value;
import org.h2.value.ValueArray;

public class CombinedResult implements ResultInterface {

    private final Session session;
    private final Query query;
    private final int maxrows;
    private final byte[] tableName;
    private ResultInterface result;
    private List<byte[]> startKeys;
    private String[] sqls;
    private int index;
    private int size;
    private JdbcConnection conn;
    private JdbcPreparedStatement ps;
    private ValueHashMap<Value[]> distinctRows;

    public CombinedResult(Session session, Query query, int maxrows) {
        this.session = session;
        this.query = query;
        this.maxrows = maxrows;
        this.tableName = HBaseUtils.toBytes(query.getTableName());
        if (!query.isDistributed()) {
            result = query.query(maxrows);
        } else {
            RowKeyConditionInfo rkci = query.getRowKeyConditionInfo();
            startKeys = rkci.getStartKeys();
            sqls = rkci.getPlanSQLs();
            size = sqls.length;
            nextResult();
        }
    }

    private void closeAllSilently() {
        if (conn != null)
            JdbcUtils.closeSilently(conn);
        if (ps != null)
            JdbcUtils.closeSilently(ps);
        if (result != null)
            result.close();
    }

    private boolean nextResult() {
        if (index >= size)
            return false;

        closeAllSilently();

        try {
            Properties info = new Properties(session.getOriginalProperties());
            HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, startKeys.get(index));
            info.setProperty("REGION_NAME", hri.getRegionName());

            //TODO 重用regionServerURL相同的连接
            conn = new JdbcConnection(hri.getRegionServerURL(), info);

            ps = (JdbcPreparedStatement) conn.prepareStatement(sqls[index]);
            ps.setMaxRows(maxrows);
            ps.setFetchSize(session.getFetchSize());
            ps.setParameters(query.getParameters());
            ps.executeQuery();
            result = ps.getInternalResultInterface();
            index++;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    public boolean containsDistinct(Value[] values) {
        if (result instanceof LocalResult)
            return ((LocalResult) result).containsDistinct(values);

        if (distinctRows == null) {
            distinctRows = ValueHashMap.newInstance();
            int visibleColumnCount = getVisibleColumnCount();
            while (next()) {
                Value[] row = currentRow();
                if (row.length > visibleColumnCount) {
                    Value[] r2 = new Value[visibleColumnCount];
                    System.arraycopy(row, 0, r2, 0, visibleColumnCount);
                    row = r2;
                }
                ValueArray array = ValueArray.get(row);
                distinctRows.put(array, row);
            }
        }

        ValueArray array = ValueArray.get(values);
        return distinctRows.get(array) != null;
    }

    public void reset() {
        result.reset();
    }

    public Value[] currentRow() {
        return result.currentRow();
    }

    public boolean next() {
        boolean next = result.next();
        if (!next) {
            next = nextResult();
            if (next)
                next = result.next();
        }
        return next;
    }

    public int getRowId() {
        return result.getRowId();
    }

    public int getVisibleColumnCount() {
        return result.getVisibleColumnCount();
    }

    public int getRowCount() {
        int count = result.getRowCount();
        while (count == 0 && nextResult())
            count = result.getRowCount();
        return count;
    }

    public boolean needToClose() {
        return result.needToClose();
    }

    public void close() {
        closeAllSilently();
    }

    public String getAlias(int i) {
        return result.getAlias(i);
    }

    public String getSchemaName(int i) {
        return result.getSchemaName(i);
    }

    public String getTableName(int i) {
        return result.getTableName(i);
    }

    public String getColumnName(int i) {
        return result.getColumnName(i);
    }

    public int getColumnType(int i) {
        return result.getColumnType(i);
    }

    public long getColumnPrecision(int i) {
        return result.getColumnPrecision(i);
    }

    public int getColumnScale(int i) {
        return result.getColumnScale(i);
    }

    public int getDisplaySize(int i) {
        return result.getDisplaySize(i);
    }

    public boolean isAutoIncrement(int i) {
        return result.isAutoIncrement(i);
    }

    public int getNullable(int i) {
        return result.getNullable(i);
    }

    public void setFetchSize(int fetchSize) {
        result.setFetchSize(fetchSize);
    }

    public int getFetchSize() {
        return result.getFetchSize();
    }

}
