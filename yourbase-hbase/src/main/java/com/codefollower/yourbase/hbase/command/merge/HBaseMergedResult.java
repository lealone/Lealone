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
package com.codefollower.yourbase.hbase.command.merge;

import java.util.List;

import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.dbobject.index.IndexType;
import com.codefollower.yourbase.dbobject.table.IndexColumn;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.value.Value;

public class HBaseMergedResult implements ResultInterface {
    private List<ResultInterface> results;
    private ResultInterface defaultResult;
    private ResultInterface currentResult;
    private int index = 0;
    private int size = 0;

    private ResultInterface mergedResult;

    public HBaseMergedResult(List<ResultInterface> results, Select newSelect, Select oldSelect, boolean isGroupQuery) {
        this.results = results;
        defaultResult = results.get(0);
        size = results.size();

        Table table = newSelect.getTopTableFilter().getTable();
        newSelect.getTopTableFilter().setIndex(
                new HBaseMergedIndex(this, table, -1, IndexColumn.wrap(table.getColumns()), IndexType.createScan(false)));

        if (isGroupQuery)
            mergedResult = newSelect.queryGroupMerge(0, null);

        mergedResult = oldSelect.calculate(mergedResult, newSelect);

        table = oldSelect.getTopTableFilter().getTable();
        oldSelect.getTopTableFilter().setIndex(
                new HBaseMergedIndex(mergedResult, table, -1, IndexColumn.wrap(table.getColumns()), IndexType
                        .createScan(false)));

        defaultResult = mergedResult = oldSelect.queryGroupMerge(0, null);
    }

    @Override
    public void reset() {
        for (ResultInterface result : results)
            result.reset();
    }

    @Override
    public Value[] currentRow() {
        if (mergedResult != null)
            return mergedResult.currentRow();
        return currentResult.currentRow();
    }

    @Override
    public boolean next() {
        if (size == 0)
            return false;

        boolean next = false;

        if (currentResult != null) {
            next = currentResult.next();
            if (next)
                return true;
        }

        if (mergedResult != null)
            return mergedResult.next();
        if (index >= size) {
            if (currentResult != null)
                currentResult.next();
            else
                return false;
        }

        if (currentResult == null && index < size) {
            currentResult = results.get(index++);
        }

        if (currentResult == null)
            return next();

        next = currentResult.next();
        if (!next) {
            currentResult = null;
            return next();
        }

        return next;
    }

    @Override
    public int getRowId() {
        return defaultResult.getRowId();
    }

    @Override
    public int getVisibleColumnCount() {
        return defaultResult.getVisibleColumnCount();
    }

    @Override
    public int getRowCount() {
        if (mergedResult != null)
            return mergedResult.getRowCount();
        int rowCount = 0;
        for (ResultInterface result : results)
            rowCount += result.getRowCount();
        return rowCount;
    }

    @Override
    public boolean needToClose() {
        return defaultResult.needToClose();
    }

    @Override
    public void close() {
        for (ResultInterface result : results)
            result.close();
    }

    @Override
    public String getAlias(int i) {
        return defaultResult.getAlias(i);
    }

    @Override
    public String getSchemaName(int i) {
        return defaultResult.getSchemaName(i);
    }

    @Override
    public String getTableName(int i) {
        return defaultResult.getTableName(i);
    }

    @Override
    public String getColumnName(int i) {
        return defaultResult.getColumnName(i);
    }

    @Override
    public int getColumnType(int i) {
        return defaultResult.getColumnType(i);
    }

    @Override
    public long getColumnPrecision(int i) {
        return defaultResult.getColumnPrecision(i);
    }

    @Override
    public int getColumnScale(int i) {
        return defaultResult.getColumnScale(i);
    }

    @Override
    public int getDisplaySize(int i) {
        return defaultResult.getDisplaySize(i);
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return defaultResult.isAutoIncrement(i);
    }

    @Override
    public int getNullable(int i) {
        return defaultResult.getNullable(i);
    }

    @Override
    public void setFetchSize(int fetchSize) {
        for (ResultInterface result : results)
            result.setFetchSize(fetchSize);
    }

    @Override
    public int getFetchSize() {
        return defaultResult.getFetchSize();
    }

}
