/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.util.List;

import org.lealone.common.trace.Trace;

/**
 * Represents a readonly ARRAY value.
 */
public class ReadonlyArray extends ArrayBase {
    {
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyArray(Value value) {
        this.value = value;
    }

    public ReadonlyArray(String value) {
        setValue(value);
    }

    @SuppressWarnings("unchecked")
    public ReadonlyArray(Object value) {
        if (value instanceof List) {
            setValue((List<String>) value);
        } else {
            setValue(value.toString());
        }
    }

    public ReadonlyArray(List<String> list) {
        setValue(list);
    }

    private void setValue(String value) {
        this.value = ValueString.get(value);
    }

    private void setValue(List<String> list) {
        int size = list.size();
        Value[] values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = ValueString.get(list.get(i));
        }
        this.value = ValueArray.get(values);
    }
}
