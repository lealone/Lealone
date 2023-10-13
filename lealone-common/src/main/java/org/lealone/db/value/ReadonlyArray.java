/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.value;

import java.util.List;

import org.lealone.common.trace.Trace;
import org.lealone.common.util.StatementBuilder;

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

    public ReadonlyArray(Object value) {
        if (value instanceof List) {
            setValue((List<?>) value);
        } else {
            setValue(value.toString());
        }
    }

    public ReadonlyArray(Object... values) {
        setValue(values);
    }

    public ReadonlyArray(List<String> list) {
        setValue(list);
    }

    private void setValue(List<?> list) {
        setValue(list.toArray());
    }

    private void setValue(Object... values) {
        value = DataType.convertToValue(values, Value.ARRAY);
    }

    private void setValue(String value) {
        this.value = ValueString.get(value);
    }

    @Override
    public String toString() {
        if (value instanceof ValueArray) {
            ValueArray va = (ValueArray) value;
            StatementBuilder buff = new StatementBuilder("[");
            for (Value v : va.getList()) {
                buff.appendExceptFirst(", ");
                buff.append(v.getString());
            }
            return buff.append(']').toString();
        }
        return value.toString();
    }
}
