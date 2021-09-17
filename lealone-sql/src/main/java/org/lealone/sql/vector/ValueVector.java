/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;

public class ValueVector {

    public BooleanVector isNull() {
        return new BooleanVector();
    }

    public BooleanVector isNotNull() {
        return new BooleanVector();
    }

    public ValueVector compare(ValueVector vv, int compareType) {
        return new BooleanVector();
    }

    public ValueVector convertTo(int targetType) {
        return this;
    }

    public ValueVector negate() {
        return null;
    }

    public ValueVector concat(ValueVector vv, boolean nullConcatIsNull) {
        return null;
    }

    public ValueVector add(ValueVector vv) {
        return null;
    }

    // 目前未使用，原本想用在sum这个聚合函数中
    public ValueVector add(ValueVector bvv0, ValueVector vv, ValueVector bvv) {
        return null;
    }

    public ValueVector subtract(ValueVector vv) {
        return null;
    }

    public ValueVector multiply(ValueVector vv) {
        return null;
    }

    public ValueVector divide(ValueVector vv) {
        return null;
    }

    public ValueVector modulus(ValueVector vv) {
        return null;
    }

    public boolean isTrue(int index) {
        return true;
    }

    public int size() {
        return 0;
    }

    public Value getValue(int index) {
        return ValueNull.INSTANCE;
    }

    public Value[] getValues(ValueVector bvv) {
        return null;
    }

    public Value sum() {
        return ValueNull.INSTANCE;
    }

    public Value sum(ValueVector bvv) {
        return ValueNull.INSTANCE;
    }

    public Value min() {
        return ValueNull.INSTANCE;
    }

    public Value min(ValueVector bvv) {
        return ValueNull.INSTANCE;
    }

    public Value max() {
        return ValueNull.INSTANCE;
    }

    public Value max(ValueVector bvv) {
        return ValueNull.INSTANCE;
    }

    public int trueCount() {
        return 0;
    }
}
