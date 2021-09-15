/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector.jdk16;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.sql.expression.condition.Comparison;
import org.lealone.sql.vector.SingleValueVector;
import org.lealone.sql.vector.ValueVector;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class Jdk16IntVector extends Jdk16ValueVector {

    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    private int[] values;

    public Jdk16IntVector(int[] values) {
        this.values = values;
    }

    @Override
    public Jdk16BooleanVector compare(ValueVector vv, int compareType) {
        switch (compareType) {
        case Comparison.EQUAL: {
            if (vv instanceof SingleValueVector) {
                int[] values1 = this.values;
                int v = ((SingleValueVector) vv).getValue().getInt();
                boolean[] values = new boolean[values1.length];
                int i = 0;
                int upperBound = SPECIES.loopBound(values.length);
                for (; i < upperBound; i += SPECIES.length()) {
                    IntVector va = IntVector.fromArray(SPECIES, this.values, i);
                    VectorMask<Integer> mask = va.compare(VectorOperators.EQ, v);
                    mask.intoArray(values, i);
                }

                for (; i < this.values.length; i++) {
                    values[i] = values1[i] == v;
                }
                return new Jdk16BooleanVector(values);
            }
            int[] values1 = this.values;
            int[] values2 = ((Jdk16IntVector) vv).values;
            boolean[] values = new boolean[values1.length];
            for (int i = 0; i < values1.length; i++) {
                values[i] = values1[i] == values2[i];
            }
            int i = 0;
            int upperBound = SPECIES.loopBound(values.length);
            for (; i < upperBound; i += SPECIES.length()) {
                IntVector va = IntVector.fromArray(SPECIES, this.values, i);
                IntVector vb = IntVector.fromArray(SPECIES, values2, i);
                VectorMask<Integer> mask = va.compare(VectorOperators.EQ, vb);
                mask.intoArray(values, i);
            }

            for (; i < this.values.length; i++) {
                values[i] = values1[i] == values2[i];
            }
            return new Jdk16BooleanVector(values);
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
                int i = 0;
                int upperBound = SPECIES.loopBound(values.length);
                for (; i < upperBound; i += SPECIES.length()) {
                    IntVector va = IntVector.fromArray(SPECIES, this.values, i);
                    VectorMask<Integer> mask = va.compare(VectorOperators.GT, v);
                    mask.intoArray(values, i);
                }

                for (; i < this.values.length; i++) {
                    values[i] = values1[i] > v;
                }
                return new Jdk16BooleanVector(values);
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
            return new Jdk16IntVector(values);
        }
        int[] values1 = this.values;
        int[] values2 = ((Jdk16IntVector) vv).values;
        int[] values = new int[values1.length];
        int i = 0;
        int len = Math.min(values1.length, values2.length);
        int upperBound = SPECIES.loopBound(len);
        for (; i < upperBound; i += SPECIES.length()) {
            IntVector va = IntVector.fromArray(SPECIES, this.values, i);
            IntVector vb = IntVector.fromArray(SPECIES, values2, i);
            IntVector vc = va.add(vb);
            vc.intoArray(values, i);
        }
        for (; i < len; i++) {
            values[i] = values1[i] + values2[i];
        }
        for (; i < values1.length; i++) {
            values[i] = values1[i];
        }
        return new Jdk16IntVector(values);
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
    public Value sum() {
        int sum = 0;
        for (int i = 0, len = values.length; i < len; i++) {
            sum += values[i];
        }
        return ValueInt.get(sum);
    }
}
