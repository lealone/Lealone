/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.expression.condition.Comparison;

public class IntVector extends ValueVector {

    private int[] values;

    public IntVector(int[] values) {
        this.values = values;
    }

    @Override
    public BooleanVector compare(ValueVector vv, int compareType) {
        switch (compareType) {
        case Comparison.EQUAL: {
            if (vv instanceof SingleValueVector) {
                int[] values1 = this.values;
                int v = ((SingleValueVector) vv).getValue().getInt();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i] == v;
                }
                return new BooleanVector(values);
            }
            int[] values1 = this.values;
            int[] values2 = ((IntVector) vv).values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i] == values2[i];
            }
            return new BooleanVector(values);
        }
        case Comparison.EQUAL_NULL_SAFE:
            return null;
        case Comparison.BIGGER_EQUAL:
            return null;
        case Comparison.BIGGER: {
            if (vv instanceof SingleValueVector) {
                int[] values1 = this.values;
                int v = ((SingleValueVector) vv).getValue().getInt();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i] > v;
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
    public ValueVector add(ValueVector vv) {
        if (vv instanceof SingleValueVector) {
            int[] values1 = this.values;
            int v = ((SingleValueVector) vv).getValue().getInt();
            int[] values = new int[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i] + v;
            }
            return new IntVector(values);
        }
        int[] values1 = this.values;
        int[] values2 = ((IntVector) vv).values;
        int[] values = new int[values1.length];
        int len = Math.min(values1.length, values2.length);
        int i = 0;
        for (; i < len; i++) {
            values[i] = values1[i] + values2[i];
        }
        for (; i < values1.length; i++) {
            values[i] = values1[i];
        }
        return new IntVector(values);
    }

    @Override
    public ValueVector add(ValueVector bvv0, ValueVector vv, ValueVector bvv) {
        return null;
    }

    @Override
    public ValueVector subtract(ValueVector vv) {
        return null;
    }

    @Override
    public ValueVector multiply(ValueVector vv) {
        return null;
    }

    @Override
    public ValueVector divide(ValueVector vv) {
        return null;
    }

    @Override
    public ValueVector modulus(ValueVector vv) {
        return null;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public Value getValue(int index) {
        return ValueInt.get(values[index]);
    }

    @Override
    public Value[] getValues(ValueVector bvv) {
        int size;
        if (bvv == null)
            size = values.length;
        else
            size = bvv.trueCount();
        Value[] a = new Value[size];
        int j = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv == null || bvv.isTrue(i))
                a[j++] = getValue(i);
        }
        return a;
    }

    @Override
    public Value sum() {
        int sum = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            sum += values[i];
        }
        return ValueInt.get(sum);
    }

    @Override
    public Value sum(ValueVector bvv) {
        if (bvv == null)
            return sum();
        int sum = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv.isTrue(i))
                sum += values[i];
        }
        return ValueInt.get(sum);
    }

    @Override
    public Value min() {
        int min = Integer.MAX_VALUE;
        for (int i = 0, len = values.length; i < len; i++) {
            if (min > values[i])
                min = values[i];
        }
        return ValueInt.get(min);
    }

    @Override
    public Value min(ValueVector bvv) {
        if (bvv == null)
            return min();
        int min = Integer.MAX_VALUE;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv.isTrue(i) && min > values[i])
                min = values[i];
        }
        return ValueInt.get(min);
    }

    @Override
    public Value max() {
        int max = Integer.MIN_VALUE;
        for (int i = 0, len = values.length; i < len; i++) {
            if (max < values[i])
                max = values[i];
        }
        return ValueInt.get(max);
    }

    @Override
    public Value max(ValueVector bvv) {
        if (bvv == null)
            return min(bvv);
        int max = Integer.MIN_VALUE;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv.isTrue(i) && max < values[i])
                max = values[i];
        }
        return ValueInt.get(max);
    }

    @Override
    public ValueVector filter(ValueVector bvv) {
        int size;
        if (bvv == null)
            size = values.length;
        else
            size = bvv.trueCount();
        int[] a = new int[size];
        int j = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv == null || bvv.isTrue(i))
                a[j++] = values[i];
        }
        return new IntVector(a);
    }
}
