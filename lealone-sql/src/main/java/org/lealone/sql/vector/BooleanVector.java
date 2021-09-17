/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBoolean;

public class BooleanVector extends ValueVector {

    private boolean[] values;

    public BooleanVector() {
    }

    public BooleanVector(boolean[] values) {
        this.values = values;
    }

    public boolean[] getValues() {
        return values;
    }

    public void setValues(boolean[] values) {
        this.values = values;
    }

    @Override
    public boolean isTrue(int index) {
        return values[index];
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value getValue(int index) {
        return ValueBoolean.get(values[index]);
    }

    @Override
    public int trueCount() {
        int c = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (values[i])
                c++;
        }
        return c;
    }
}
