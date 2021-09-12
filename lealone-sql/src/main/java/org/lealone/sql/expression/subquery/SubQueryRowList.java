/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.subquery;

import java.util.ArrayList;

import org.lealone.db.result.DelegatedResult;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;

class SubQueryRowList extends DelegatedResult {

    final ArrayList<Value[]> rowList;
    final int size;
    int index;

    SubQueryRowList(ArrayList<Value[]> rowList, Result result) {
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
