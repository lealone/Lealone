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
import com.codefollower.yourbase.util.ValueHashMap;
import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueArray;

public class SubqueryResult extends DelegatedResult {
    protected ValueHashMap<Value[]> distinctRows;
    protected int rowCount = -1;

    public SubqueryResult() {
    }

    public SubqueryResult(Query query, int maxrows) {
        result = query.query(maxrows);
    }

    public boolean containsDistinct(Value[] values) {
        if (result instanceof LocalResult)
            return ((LocalResult) result).containsDistinct(values);

        if (distinctRows == null) {
            initDistinctRows();
        }

        ValueArray array = ValueArray.get(values);
        return distinctRows.get(array) != null;
    }

    private int initDistinctRows() {
        if (distinctRows == null) {
            rowCount = 0;

            distinctRows = ValueHashMap.newInstance();
            int visibleColumnCount = getVisibleColumnCount();
            while (next()) {
                rowCount++;
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

        return rowCount;
    }

    @Override
    public int getRowCount() {
        int rowCount = result.getRowCount();
        if (rowCount == -1) {
            initDistinctRows();
            return this.rowCount;
        }

        return rowCount;
    }
}
