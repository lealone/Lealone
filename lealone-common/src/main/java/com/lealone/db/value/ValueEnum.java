/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.value;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.MathUtils;
import com.lealone.db.api.ErrorCode;

/**
 * Implementation of the ENUM data type.
 */
public class ValueEnum extends Value {

    private String label;
    private final int value;

    private ValueEnum(String label, int value) {
        this.label = label;
        this.value = value;
    }

    private ValueEnum(int value) {
        this.label = null;
        this.value = value;
    }

    public static ValueEnum get(int ordinal) {
        return new ValueEnum(null, ordinal);
    }

    public static ValueEnum get(String label, int ordinal) {
        return new ValueEnum(label, ordinal);
    }

    @Override
    public int getType() {
        return Value.ENUM;
    }

    @Override
    public Value add(Value v) {
        ValueEnum other = (ValueEnum) v;
        return checkRange(value + (long) other.value);
    }

    private static ValueEnum checkRange(long x) {
        if (x < Integer.MIN_VALUE || x > Integer.MAX_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(x));
        }
        return ValueEnum.get((int) x);
    }

    @Override
    public int getSignum() {
        return Integer.signum(value);
    }

    @Override
    public Value negate() {
        return checkRange(-(long) value);
    }

    @Override
    public Value subtract(Value v) {
        ValueEnum other = (ValueEnum) v;
        return checkRange(value - (long) other.value);
    }

    @Override
    public Value multiply(Value v) {
        ValueEnum other = (ValueEnum) v;
        return checkRange(value * (long) other.value);
    }

    @Override
    public Value divide(Value v) {
        ValueEnum other = (ValueEnum) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueEnum.get(value / other.value);
    }

    @Override
    public Value modulus(Value v) {
        ValueEnum other = (ValueEnum) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueEnum.get(value % other.value);
    }

    @Override
    public String getSQL() {
        return getString();
    }

    @Override
    public int getInt() {
        return value;
    }

    @Override
    public long getLong() {
        return value;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueEnum v = (ValueEnum) o;
        if (label != null && v.label != null)
            return mode.compareString(label, v.label, false);
        return MathUtils.compareInt(value, v.value);
    }

    @Override
    public String getString() {
        return label;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public Object getObject() {
        return label;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setInt(parameterIndex, value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueEnum && value == ((ValueEnum) other).value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public int getMemory() {
        return 24;
    }
}
