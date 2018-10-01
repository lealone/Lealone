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
package org.lealone.sql.expression;

import java.util.ArrayList;

import org.lealone.db.result.DelegatedResult;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.sql.dml.Query;

public class SubqueryResult extends DelegatedResult {
    protected ValueHashMap<Value[]> distinctRows;
    protected int rowCount = -1;

    public SubqueryResult() {
    }

    public SubqueryResult(Query query, int maxRows) {
        query.setLocal(false);
        result = query.query(maxRows);
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
            ArrayList<Value[]> rowList = new ArrayList<>();
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
                rowList.add(row);
            }

            result = new SubqueryRowList(rowList, result);
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

    private static class SubqueryRowList extends DelegatedResult {
        final ArrayList<Value[]> rowList;
        final int size;
        int index;

        SubqueryRowList(ArrayList<Value[]> rowList, Result result) {
            this.result = result;
            this.rowList = rowList;
            index = -1;
            size = rowList.size();
        }

        @Override
        public void reset() {
            index = -1;
        }

        @Override
        public Value[] currentRow() {
            return rowList.get(index);
        }

        @Override
        public boolean next() {
            return ++index < size;
        }

        @Override
        public int getRowCount() {
            return size;
        }
    }
}
