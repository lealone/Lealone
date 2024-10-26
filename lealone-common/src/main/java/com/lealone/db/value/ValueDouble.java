/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.api.ErrorCode;

/**
 * Implementation of the DOUBLE data type.
 */
public class ValueDouble extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 17;

    /**
     * The maximum display size of a double.
     * Example: -3.3333333333333334E-100
     */
    public static final int DISPLAY_SIZE = 24;

    /**
     * Double.doubleToLongBits(0.0)
     */
    public static final long ZERO_BITS = Double.doubleToLongBits(0.0);

    private static final ValueDouble ZERO = new ValueDouble(0.0);
    private static final ValueDouble ONE = new ValueDouble(1.0);
    private static final ValueDouble NAN = new ValueDouble(Double.NaN);

    private final double value;

    private ValueDouble(double value) {
        this.value = value;
    }

    @Override
    public Value add(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value + v2.value);
    }

    @Override
    public Value subtract(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value - v2.value);
    }

    @Override
    public Value negate() {
        return ValueDouble.get(-value);
    }

    @Override
    public Value multiply(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        return ValueDouble.get(value * v2.value);
    }

    @Override
    public Value divide(Value v) {
        ValueDouble v2 = (ValueDouble) v;
        if (v2.value == 0.0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value / v2.value);
    }

    @Override
    public ValueDouble modulus(Value v) {
        ValueDouble other = (ValueDouble) v;
        if (other.value == 0) {
            throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
        }
        return ValueDouble.get(value % other.value);
    }

    @Override
    public String getSQL() {
        if (value == Double.POSITIVE_INFINITY) {
            return "POWER(0, -1)";
        } else if (value == Double.NEGATIVE_INFINITY) {
            return "(-POWER(0, -1))";
        } else if (Double.isNaN(value)) {
            return "SQRT(-1)";
        }
        String s = getString();
        if (s.equals("-0.0")) {
            return "-CAST(0 AS DOUBLE)";
        }
        return s;
    }

    @Override
    public int getType() {
        return Value.DOUBLE;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        ValueDouble v = (ValueDouble) o;
        return Double.compare(value, v.value);
    }

    @Override
    public int getSignum() {
        return value == 0 ? 0 : (value < 0 ? -1 : 1);
    }

    @Override
    public double getDouble() {
        return value;
    }

    @Override
    public String getString() {
        return String.valueOf(value);
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int hashCode() {
        long hash = Double.doubleToLongBits(value);
        return (int) (hash ^ (hash >> 32));
    }

    @Override
    public Object getObject() {
        return Double.valueOf(value);
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDouble(parameterIndex, value);
    }

    /**
     * Get or create double value for the given double.
     *
     * @param d the double
     * @return the value
     */
    public static ValueDouble get(double d) {
        if (d == 1.0) {
            return ONE;
        } else if (d == 0.0) {
            // unfortunately, -0.0 == 0.0, but we don't want to return
            // 0.0 in this case
            if (Double.doubleToLongBits(d) == ZERO_BITS) {
                return ZERO;
            }
        } else if (Double.isNaN(d)) {
            return NAN;
        }
        return (ValueDouble) Value.cache(new ValueDouble(d));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ValueDouble)) {
            return false;
        }
        return compareSecure((ValueDouble) other, null) == 0;
    }

    public static final ValueDataTypeBase type = new ValueDataTypeBase() {

        @Override
        public int getType() {
            return DOUBLE;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Double a = (Double) aObj;
            Double b = (Double) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 24;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            double x = (Double) obj;
            write0(buff, x);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            double x = v.getDouble();
            write0(buff, x);
        }

        private void write0(DataBuffer buff, double x) {
            long d = Double.doubleToLongBits(x);
            if (d == DOUBLE_ZERO_BITS) {
                buff.put((byte) TAG_DOUBLE_0);
            } else if (d == DOUBLE_ONE_BITS) {
                buff.put((byte) TAG_DOUBLE_1);
            } else {
                long value = Long.reverse(d);
                if (value >= 0 && value <= DataUtils.COMPRESSED_VAR_LONG_MAX) {
                    buff.put((byte) DOUBLE);
                    buff.putVarLong(value);
                } else {
                    buff.put((byte) TAG_DOUBLE_FIXED);
                    buff.putDouble(x);
                }
            }
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            switch (tag) {
            case TAG_DOUBLE_0:
                return ValueDouble.get(0d);
            case TAG_DOUBLE_1:
                return ValueDouble.get(1d);
            case TAG_DOUBLE_FIXED:
                return ValueDouble.get(buff.getDouble());
            }
            return ValueDouble.get(Double.longBitsToDouble(Long.reverse(DataUtils.readVarLong(buff))));
        }
    };
}
