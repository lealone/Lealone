/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.subquery;

import java.util.ArrayList;

import com.lealone.db.result.DelegatedResult;
import com.lealone.db.result.LocalResult;
import com.lealone.db.util.ValueHashMap;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.sql.query.Query;

public class SubQueryResult extends DelegatedResult {

    protected ValueHashMap<Value[]> distinctRows;
    protected int rowCount = -1;

    public SubQueryResult(Query query, int maxRows) {
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

            result = new SubQueryRowList(rowList, result);
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
