/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.condition.Comparison;

public class DefaultValueVector extends ValueVector {

    private Value[] values;

    public DefaultValueVector(Value[] values) {
        this.values = values;
    }

    @Override
    public BooleanVector compare(ValueVector vv, int compareType) {
        switch (compareType) {
        case Comparison.EQUAL: {
            if (vv instanceof SingleValueVector) {
                Value[] values1 = this.values;
                Value v = ((SingleValueVector) vv).getValue();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i].compareTo(v) == 0;
                }
                return new BooleanVector(values);
            }
            Value[] values1 = this.values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i].compareTo(vv.getValue(i)) == 0;
            }
            return new BooleanVector(values);
        }
        case Comparison.EQUAL_NULL_SAFE:
            return null;
        case Comparison.BIGGER_EQUAL: {
            if (vv instanceof SingleValueVector) {
                Value[] values1 = this.values;
                Value v = ((SingleValueVector) vv).getValue();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i].compareTo(v) >= 0;
                }
                return new BooleanVector(values);
            }
            Value[] values1 = this.values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i].compareTo(vv.getValue(i)) >= 0;
            }
            return new BooleanVector(values);
        }
        case Comparison.BIGGER: {
            if (vv instanceof SingleValueVector) {
                Value[] values1 = this.values;
                Value v = ((SingleValueVector) vv).getValue();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i].compareTo(v) > 0;
                }
                return new BooleanVector(values);
            }
            return null;
        }
        case Comparison.SMALLER_EQUAL:
            return null;
        case Comparison.SMALLER:
            return null;
        case Comparison.NOT_EQUAL:
            return null;
        case Comparison.NOT_EQUAL_NULL_SAFE:
            return null;
        default:
            throw DbException.getInternalError("compareType=" + compareType);
        }
    }

    @Override
    public Value getValue(int index) {
        return values[index];
    }

    @Override
    public ValueVector filter(ValueVector bvv) {
        int size;
        if (bvv == null)
            size = values.length;
        else
            size = bvv.trueCount();
        Value[] a = new Value[size];
        int j = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv == null || bvv.isTrue(i))
                a[j++] = values[i];
        }
        return new DefaultValueVector(a);
    }
}
