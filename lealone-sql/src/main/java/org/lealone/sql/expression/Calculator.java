/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression;

import java.util.ArrayList;
import java.util.List;

import org.lealone.db.value.Value;

public class Calculator {
    private final Value[] currentRow;
    private final List<Value> result;
    private int index;

    public Calculator(Value[] currentRow) {
        this(currentRow, 0);
    }

    public Calculator(Value[] currentRow, int index) {
        this.currentRow = currentRow;
        this.index = index;
        this.result = new ArrayList<Value>(currentRow.length - 2);
    }

    public int getIndex() {
        return index;
    }

    public int addIndex() {
        return ++index;
    }

    public int addIndex(int i) {
        index += i;
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public List<Value> getResult() {
        return result;
    }

    public void addResultValue(Value v) {
        result.add(v);
    }

    public Value getValue(int i) {
        return currentRow[i];
    }
}
