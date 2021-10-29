/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.condition.Comparison;

public class StringVector extends ValueVector {

    private String[] values;

    public StringVector(String[] values) {
        this.values = values;
    }

    @Override
    public BooleanVector compare(ValueVector vv, int compareType) {
        switch (compareType) {
        case Comparison.EQUAL: {
            if (vv instanceof SingleValueVector) {
                String[] values1 = this.values;
                String v = ((SingleValueVector) vv).getValue().getString();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i].compareTo(v) == 0;
                }
                return new BooleanVector(values);
            }
            String[] values1 = this.values;
            String[] values2 = ((StringVector) vv).values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i].compareTo(values2[i]) == 0;
            }
            return new BooleanVector(values);
        }
        case Comparison.EQUAL_NULL_SAFE:
            return null;
        case Comparison.BIGGER_EQUAL: {
            if (vv instanceof SingleValueVector) {
                String[] values1 = this.values;
                String v = ((SingleValueVector) vv).getValue().getString();
                boolean[] values = new boolean[values1.length];
                for (int i = 0; i < values1.length; i++) {
                    values[i] = values1[i].compareTo(v) >= 0;
                }
                return new BooleanVector(values);
            }
            String[] values1 = this.values;
            String[] values2 = ((StringVector) vv).values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i].compareTo(values2[i]) >= 0;
            }
            return new BooleanVector(values);
        }
        case Comparison.BIGGER: {
            if (vv instanceof SingleValueVector) {
                String[] values1 = this.values;
                String v = ((SingleValueVector) vv).getValue().getString();
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
    public ValueVector add(ValueVector vv) {
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
        return ValueString.get(values[index]);
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
        return null;
    }

    @Override
    public ValueVector filter(ValueVector bvv) {
        int size;
        if (bvv == null)
            size = values.length;
        else
            size = bvv.trueCount();
        String[] a = new String[size];
        int j = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            if (bvv == null || bvv.isTrue(i))
                a[j++] = values[i];
        }
        return new StringVector(a);
    }
}
