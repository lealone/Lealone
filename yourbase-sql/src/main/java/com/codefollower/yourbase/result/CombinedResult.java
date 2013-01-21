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
package com.codefollower.yourbase.result;

import com.codefollower.yourbase.command.dml.Query;
import com.codefollower.yourbase.result.LocalResult;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.util.ValueHashMap;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueArray;

public class CombinedResult implements ResultInterface {
    private ResultInterface result;
    private ValueHashMap<Value[]> distinctRows;

    public CombinedResult() {
    }

    public CombinedResult(Query query, int maxrows) {
        result = query.query(maxrows);
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
        return result.next();
    }

    public int getRowId() {
        return result.getRowId();
    }

    public int getVisibleColumnCount() {
        return result.getVisibleColumnCount();
    }

    public int getRowCount() {
        return result.getRowCount();
    }

    public boolean needToClose() {
        return result.needToClose();
    }

    public void close() {
        result.close();
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
