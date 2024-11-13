/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.UUID;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DateTimeUtils;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Constants;
import com.lealone.db.SysProperties;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.result.SimpleResultSet;

/**
 * This is the base class for all value classes.
 * It provides conversion and comparison methods.
 */
public abstract class Value implements Comparable<Value> {

    /**
     * The data type is unknown at this time.
     */
    public static final int UNKNOWN = -1;

    /**
     * The value type for NULL.
     */
    public static final int NULL = 0;

    /**
     * The value type for BOOLEAN values.
     */
    public static final int BOOLEAN = 1;

    /**
     * The value type for BYTE values.
     */
    public static final int BYTE = 2;

    /**
     * The value type for SHORT values.
     */
    public static final int SHORT = 3;

    /**
     * The value type for INT values.
     */
    public static final int INT = 4;

    /**
     * The value type for LONG values.
     */
    public static final int LONG = 5;

    /**
     * The value type for DECIMAL values.
     */
    public static final int DECIMAL = 6;

    /**
     * The value type for DOUBLE values.
     */
    public static final int DOUBLE = 7;

    /**
     * The value type for FLOAT values.
     */
    public static final int FLOAT = 8;

    /**
     * The value type for TIME values.
     */
    public static final int TIME = 9;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = 10;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = 11;

    /**
     * The value type for BYTES values.
     */
    public static final int BYTES = 12;

    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;

    /**
     * The value type for case insensitive STRING values.
     */
    public static final int STRING_IGNORECASE = 14;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = 15;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = 16;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = 17;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = 18;

    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = 19;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = 20;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int STRING_FIXED = 21;

    public static final int LIST = 22;
    public static final int SET = 23;
    public static final int MAP = 24;
    public static final int ENUM = 25;

    /**
     * The number of value types.
     */
    public static final int TYPE_COUNT = ENUM + 1;

    private static SoftReference<Value[]> softCache = new SoftReference<Value[]>(null);
    private static final BigDecimal MAX_LONG_DECIMAL = BigDecimal.valueOf(Long.MAX_VALUE);
    private static final BigDecimal MIN_LONG_DECIMAL = BigDecimal.valueOf(Long.MIN_VALUE);

    private static final CompareMode COMPARE_MODE = CompareMode.getInstance(null, 0, false);

    @Override
    public int compareTo(Value o) {
        return compareTo(o, COMPARE_MODE);
    }

    /**
     * Get the SQL expression for this value.
     *
     * @return the SQL expression
     */
    public abstract String getSQL();

    /**
     * Get the value type.
     *
     * @return the type
     */
    public abstract int getType();

    /**
     * Get the precision.
     *
     * @return the precision
     */
    public abstract long getPrecision();

    /**
     * Get the display size in characters.
     *
     * @return the display size
     */
    public abstract int getDisplaySize();

    /**
     * Get the memory used by this object.
     *
     * @return the memory used in bytes
     */
    public abstract int getMemory();

    /**
     * Get the value as a string.
     *
     * @return the string
     */
    public abstract String getString();

    /**
     * Get the value as an object.
     *
     * @return the object
     */
    public abstract Object getObject();

    /**
     * Set the value as a parameter in a prepared statement.
     *
     * @param prep the prepared statement
     * @param parameterIndex the parameter index
     */
    public abstract void set(PreparedStatement prep, int parameterIndex) throws SQLException;

    /**
     * Compare the value with another value of the same type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    protected abstract int compareSecure(Value v, CompareMode mode);

    @Override
    public abstract int hashCode();

    /**
     * Check if the two values have the same hash code. No data conversion is
     * made; this method returns false if the other object is not of the same
     * class. For some values, compareTo may return 0 even if equals return
     * false. Example: ValueDecimal 0.0 and 0.00.
     *
     * @param other the other value
     * @return true if they are equal
     */
    @Override
    public abstract boolean equals(Object other);

    /**
     * Get the order of this value type.
     *
     * @param type the value type
     * @return the order number
     */
    static int getOrder(int type) {
        switch (type) {
        case UNKNOWN:
            return 1;
        case NULL:
            return 2;
        case STRING:
            return 10;
        case CLOB:
            return 11;
        case STRING_FIXED:
            return 12;
        case STRING_IGNORECASE:
            return 13;
        case BOOLEAN:
            return 20;
        case BYTE:
            return 21;
        case SHORT:
            return 22;
        case INT:
            return 23;
        case LONG:
            return 24;
        case DECIMAL:
            return 25;
        case FLOAT:
            return 26;
        case DOUBLE:
            return 27;
        case TIME:
            return 30;
        case DATE:
            return 31;
        case TIMESTAMP:
            return 32;
        case BYTES:
            return 40;
        case BLOB:
            return 41;
        case UUID:
            return 42;
        case JAVA_OBJECT:
            return 43;
        case ARRAY:
            return 50;
        case RESULT_SET:
            return 51;
        case LIST:
            return 52;
        case SET:
            return 53;
        case MAP:
            return 54;
        case ENUM:
            return 55;
        default:
            throw DbException.getInternalError("type:" + type);
        }
    }

    /**
     * Get the higher value order type of two value types. If values need to be
     * converted to match the other operands value type, the value with the
     * lower order is converted to the value with the higher order.
     *
     * @param t1 the first value type
     * @param t2 the second value type
     * @return the higher value type of the two
     */
    public static int getHigherOrder(int t1, int t2) {
        if (t1 == Value.UNKNOWN || t2 == Value.UNKNOWN) {
            if (t1 == t2) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, ?");
            } else if (t1 == Value.NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "NULL, ?");
            } else if (t2 == Value.NULL) {
                throw DbException.get(ErrorCode.UNKNOWN_DATA_TYPE_1, "?, NULL");
            }
        }
        if (t1 == t2) {
            return t1;
        }
        int o1 = getOrder(t1);
        int o2 = getOrder(t2);
        return o1 > o2 ? t1 : t2;
    }

    /**
     * Check if a value is in the cache that is equal to this value. If yes,
     * this value should be used to save memory. If the value is not in the
     * cache yet, it is added.
     *
     * @param v the value to look for
     * @return the value in the cache or the value passed
     */
    static Value cache(Value v) {
        if (SysProperties.OBJECT_CACHE) {
            int hash = v.hashCode();
            if (softCache == null) {
                softCache = new SoftReference<Value[]>(null);
            }
            Value[] cache = softCache.get();
            if (cache == null) {
                cache = new Value[SysProperties.OBJECT_CACHE_SIZE];
                softCache = new SoftReference<Value[]>(cache);
            }
            int index = hash & (SysProperties.OBJECT_CACHE_SIZE - 1);
            Value cached = cache[index];
            if (cached != null) {
                if (cached.getType() == v.getType() && v.equals(cached)) {
                    // cacheHit++;
                    return cached;
                }
            }
            // cacheMiss++;
            // cache[cacheCleaner] = null;
            // cacheCleaner = (cacheCleaner + 1) & (Constants.OBJECT_CACHE_SIZE - 1);
            cache[index] = v;
        }
        return v;
    }

    /**
     * Clear the value cache. Used for testing.
     */
    public static void clearCache() {
        softCache = null;
    }

    public boolean getBoolean() {
        return ((ValueBoolean) convertTo(Value.BOOLEAN)).getBoolean();
    }

    public Date getDate() {
        return ((ValueDate) convertTo(Value.DATE)).getDate();
    }

    public Time getTime() {
        return ((ValueTime) convertTo(Value.TIME)).getTime();
    }

    public Timestamp getTimestamp() {
        return ((ValueTimestamp) convertTo(Value.TIMESTAMP)).getTimestamp();
    }

    public byte[] getBytes() {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytes();
    }

    public byte[] getBytesNoCopy() {
        return ((ValueBytes) convertTo(Value.BYTES)).getBytesNoCopy();
    }

    public byte getByte() {
        return ((ValueByte) convertTo(Value.BYTE)).getByte();
    }

    public short getShort() {
        return ((ValueShort) convertTo(Value.SHORT)).getShort();
    }

    public BigDecimal getBigDecimal() {
        return ((ValueDecimal) convertTo(Value.DECIMAL)).getBigDecimal();
    }

    public double getDouble() {
        return ((ValueDouble) convertTo(Value.DOUBLE)).getDouble();
    }

    public float getFloat() {
        return ((ValueFloat) convertTo(Value.FLOAT)).getFloat();
    }

    public int getInt() {
        return ((ValueInt) convertTo(Value.INT)).getInt();
    }

    public long getLong() {
        return ((ValueLong) convertTo(Value.LONG)).getLong();
    }

    public UUID getUuid() {
        return ((ValueUuid) convertTo(Value.UUID)).getUuid();
    }

    public InputStream getInputStream() {
        return new ByteArrayInputStream(getBytesNoCopy());
    }

    public Reader getReader() {
        return new StringReader(getString());
    }

    public Blob getBlob() {
        return new ReadonlyBlob(ValueLob.createSmallLob(Value.BLOB, getBytesNoCopy()));
    }

    public Clob getClob() {
        return new ReadonlyClob(
                ValueLob.createSmallLob(Value.CLOB, getString().getBytes(Constants.UTF8)));
    }

    public Array getArray() {
        return new ReadonlyArray(ValueArray.get(new Value[] { ValueString.get(getString()) }));
    }

    /**
     * Add a value and return the result.
     *
     * @param v the value to add
     * @return the result
     */
    public Value add(Value v) {
        throw throwUnsupportedExceptionForType("+");
    }

    public int getSignum() {
        throw throwUnsupportedExceptionForType("SIGNUM");
    }

    /**
     * Return -value if this value support arithmetic operations.
     *
     * @return the negative
     */
    public Value negate() {
        throw throwUnsupportedExceptionForType("NEG");
    }

    /**
     * Subtract a value and return the result.
     *
     * @param v the value to subtract
     * @return the result
     */
    public Value subtract(Value v) {
        throw throwUnsupportedExceptionForType("-");
    }

    /**
     * Divide by a value and return the result.
     *
     * @param v the value to divide by
     * @return the result
     */
    public Value divide(Value v) {
        throw throwUnsupportedExceptionForType("/");
    }

    /**
     * Multiply with a value and return the result.
     *
     * @param v the value to multiply with
     * @return the result
     */
    public Value multiply(Value v) {
        throw throwUnsupportedExceptionForType("*");
    }

    /**
     * Take the modulus with a value and return the result.
     *
     * @param v the value to take the modulus with
     * @return the result
     */
    public Value modulus(Value v) {
        throw throwUnsupportedExceptionForType("%");
    }

    private Value toBoolean() {
        switch (getType()) {
        case BYTE:
        case SHORT:
        case INT:
        case LONG:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
            return ValueBoolean.get(getSignum() != 0);
        case TIME:
        case DATE:
        case TIMESTAMP:
        case BYTES:
        case JAVA_OBJECT:
        case UUID:
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, getString());
        default:
            String s = getString();
            if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("t") || s.equalsIgnoreCase("yes")
                    || s.equalsIgnoreCase("y")) {
                return ValueBoolean.get(true);
            } else if (s.equalsIgnoreCase("false") || s.equalsIgnoreCase("f") || s.equalsIgnoreCase("no")
                    || s.equalsIgnoreCase("n")) {
                return ValueBoolean.get(false);
            } else {
                // convert to a number, and if it is not 0 then it is true
                return ValueBoolean.get(new BigDecimal(s).signum() != 0);
            }
        }
    }

    private Value toByte() {
        switch (getType()) {
        case BOOLEAN:
            return ValueByte.get(getBoolean() ? (byte) 1 : (byte) 0);
        case SHORT:
            return ValueByte.get(convertToByte(getShort()));
        case INT:
            return ValueByte.get(convertToByte(getInt()));
        case LONG:
            return ValueByte.get(convertToByte(getLong()));
        case DECIMAL:
            return ValueByte.get(convertToByte(convertToLong(getBigDecimal())));
        case DOUBLE:
            return ValueByte.get(convertToByte(convertToLong(getDouble())));
        case FLOAT:
            return ValueByte.get(convertToByte(convertToLong(getFloat())));
        case BYTES:
            return ValueByte.get((byte) Integer.parseInt(getString(), 16));
        default:
            return ValueByte.get(Byte.parseByte(getString()));
        }
    }

    private String getTrimmedString() {
        return getString().trim();
    }

    private Value toShort() {
        switch (getType()) {
        case BOOLEAN:
            return ValueShort.get(getBoolean() ? (short) 1 : (short) 0);
        case BYTE:
            return ValueShort.get(getByte());
        case INT:
            return ValueShort.get(convertToShort(getInt()));
        case LONG:
            return ValueShort.get(convertToShort(getLong()));
        case DECIMAL:
            return ValueShort.get(convertToShort(convertToLong(getBigDecimal())));
        case DOUBLE:
            return ValueShort.get(convertToShort(convertToLong(getDouble())));
        case FLOAT:
            return ValueShort.get(convertToShort(convertToLong(getFloat())));
        case BYTES:
            return ValueShort.get((short) Integer.parseInt(getString(), 16));
        default:
            return ValueShort.get(Short.parseShort(getTrimmedString()));
        }
    }

    private Value toInt() {
        switch (getType()) {
        case BOOLEAN:
            return ValueInt.get(getBoolean() ? 1 : 0);
        case BYTE:
            return ValueInt.get(getByte());
        case SHORT:
            return ValueInt.get(getShort());
        case LONG:
            return ValueInt.get(convertToInt(getLong()));
        case DECIMAL:
            return ValueInt.get(convertToInt(convertToLong(getBigDecimal())));
        case DOUBLE:
            return ValueInt.get(convertToInt(convertToLong(getDouble())));
        case FLOAT:
            return ValueInt.get(convertToInt(convertToLong(getFloat())));
        case BYTES:
            return ValueInt.get((int) Long.parseLong(getString(), 16));
        default:
            return ValueInt.get(Integer.parseInt(getTrimmedString()));
        }
    }

    private Value toLong() {
        switch (getType()) {
        case BOOLEAN:
            return ValueLong.get(getBoolean() ? 1 : 0);
        case BYTE:
            return ValueLong.get(getByte());
        case SHORT:
            return ValueLong.get(getShort());
        case INT:
            return ValueLong.get(getInt());
        case DECIMAL:
            return ValueLong.get(convertToLong(getBigDecimal()));
        case DOUBLE:
            return ValueLong.get(convertToLong(getDouble()));
        case FLOAT:
            return ValueLong.get(convertToLong(getFloat()));
        case BYTES:
            // parseLong doesn't work for ffffffffffffffff
            byte[] d = getBytes();
            if (d.length == 8) {
                return ValueLong.get(Utils.readLong(d, 0));
            }
            return ValueLong.get(Long.parseLong(getString(), 16));
        default:
            return ValueLong.get(Long.parseLong(getTrimmedString()));
        }
    }

    private Value toDecimal() {
        switch (getType()) {
        case BOOLEAN:
            return ValueDecimal.get(BigDecimal.valueOf(getBoolean() ? 1 : 0));
        case BYTE:
            return ValueDecimal.get(BigDecimal.valueOf(getByte()));
        case SHORT:
            return ValueDecimal.get(BigDecimal.valueOf(getShort()));
        case INT:
            return ValueDecimal.get(BigDecimal.valueOf(getInt()));
        case LONG:
            return ValueDecimal.get(BigDecimal.valueOf(getLong()));
        case DOUBLE:
            double d = getDouble();
            if (Double.isInfinite(d) || Double.isNaN(d)) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "" + d);
            }
            return ValueDecimal.get(BigDecimal.valueOf(d));
        case FLOAT:
            float f = getFloat();
            if (Float.isInfinite(f) || Float.isNaN(f)) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, "" + f);
            }
            // better rounding behavior than BigDecimal.valueOf(f)
            return ValueDecimal.get(new BigDecimal(Float.toString(f)));
        default:
            return ValueDecimal.get(new BigDecimal(getTrimmedString()));
        }
    }

    private Value toDouble() {
        switch (getType()) {
        case BOOLEAN:
            return ValueDouble.get(getBoolean() ? 1 : 0);
        case BYTE:
            return ValueDouble.get(getByte());
        case SHORT:
            return ValueDouble.get(getShort());
        case INT:
            return ValueDouble.get(getInt());
        case LONG:
            return ValueDouble.get(getLong());
        case DECIMAL:
            return ValueDouble.get(getBigDecimal().doubleValue());
        case FLOAT:
            return ValueDouble.get(getFloat());
        default:
            return ValueDouble.get(Double.parseDouble(getTrimmedString()));
        }
    }

    private Value toFloat() {
        switch (getType()) {
        case BOOLEAN:
            return ValueFloat.get(getBoolean() ? 1 : 0);
        case BYTE:
            return ValueFloat.get(getByte());
        case SHORT:
            return ValueFloat.get(getShort());
        case INT:
            return ValueFloat.get(getInt());
        case LONG:
            return ValueFloat.get(getLong());
        case DECIMAL:
            return ValueFloat.get(getBigDecimal().floatValue());
        case DOUBLE:
            return ValueFloat.get((float) getDouble());
        default:
            return ValueFloat.get(Float.parseFloat(getTrimmedString()));
        }
    }

    private Value toDate() {
        switch (getType()) {
        case TIME:
            // because the time has set the date to 1970-01-01,
            // this will be the result
            return ValueDate.fromDateValue(DateTimeUtils.dateValue(1970, 1, 1));
        case TIMESTAMP:
            return ValueDate.fromDateValue(((ValueTimestamp) this).getDateValue());
        default:
            return ValueDate.parse(getTrimmedString());
        }
    }

    private Value toTime() {
        switch (getType()) {
        case DATE:
            // need to normalize the year, month and day
            // because a date has the time set to 0, the result will be 0
            return ValueTime.fromNanos(0);
        case TIMESTAMP:
            return ValueTime.fromNanos(((ValueTimestamp) this).getNanos());
        default:
            return ValueTime.parse(getTrimmedString());
        }
    }

    private Value toTimestamp() {
        switch (getType()) {
        case TIME:
            return DateTimeUtils.normalizeTimestamp(0, ((ValueTime) this).getNanos());
        case DATE:
            return ValueTimestamp.fromDateValueAndNanos(((ValueDate) this).getDateValue(), 0);
        default:
            return ValueTimestamp.parse(getTrimmedString());
        }
    }

    private Value toBytes() {
        switch (getType()) {
        case JAVA_OBJECT:
        case BLOB:
            return ValueBytes.getNoCopy(getBytesNoCopy());
        case UUID:
            return ValueBytes.getNoCopy(getBytes());
        case BYTE:
            return ValueBytes.getNoCopy(new byte[] { getByte() });
        case SHORT: {
            int x = getShort();
            return ValueBytes.getNoCopy(new byte[] { (byte) (x >> 8), (byte) x });
        }
        case INT: {
            int x = getInt();
            return ValueBytes.getNoCopy(
                    new byte[] { (byte) (x >> 24), (byte) (x >> 16), (byte) (x >> 8), (byte) x });
        }
        case LONG: {
            long x = getLong();
            return ValueBytes.getNoCopy(new byte[] {
                    (byte) (x >> 56),
                    (byte) (x >> 48),
                    (byte) (x >> 40),
                    (byte) (x >> 32),
                    (byte) (x >> 24),
                    (byte) (x >> 16),
                    (byte) (x >> 8),
                    (byte) x });
        }
        default:
            return ValueBytes.getNoCopy(StringUtils.convertHexToBytes(getTrimmedString()));
        }
    }

    private Value toJavaObject() {
        switch (getType()) {
        case BYTES:
        case BLOB:
            return ValueJavaObject.getNoCopy(null, getBytesNoCopy());
        default:
            return this; // 其他类型直接返回
        // return ValueJavaObject.getNoCopy(null, StringUtils.convertHexToBytes(getTrimmedString()));
        }
    }

    private Value toBlob() {
        switch (getType()) {
        case BYTES:
            return ValueLob.createSmallLob(BLOB, getBytesNoCopy());
        default:
            return ValueLob.createSmallLob(BLOB, getString().getBytes(Constants.UTF8));
        }
    }

    private Value toClob() {
        return ValueLob.createSmallLob(CLOB, getString().getBytes(Constants.UTF8));
    }

    private Value toUuid() {
        switch (getType()) {
        case BYTES:
            return ValueUuid.get(getBytesNoCopy());
        default:
            return ValueUuid.get(getString());
        }
    }

    private Value toArray() {
        Value[] values = { this };
        return ValueArray.get(values);
    }

    private Value toList() {
        switch (getType()) {
        case ARRAY:
            return ValueList.get(((ValueArray) this).getList());
        default:
            Value[] values = { this };
            return ValueList.get(values);
        }
    }

    private Value toSet() {
        switch (getType()) {
        case ARRAY:
            return ValueSet.get(((ValueArray) this).getList());
        default:
            Value[] values = { this };
            return ValueSet.get(values);
        }
    }

    private Value toMap() {
        switch (getType()) {
        case ARRAY:
            return ValueMap.get(((ValueArray) this).getList());
        default:
            Value[] values = { this };
            return ValueMap.get(values);
        }
    }

    private Value toEnum() {
        switch (getType()) {
        case STRING:
        case STRING_IGNORECASE:
        case STRING_FIXED:
            String label = getString();
            return ValueEnum.get(label, 0);
        default:
            return ValueEnum.get(getInt());
        }
    }

    private Value toResultSet() {
        String s = getString();
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", Types.VARCHAR, s.length(), 0);
        rs.addRow(s);
        return ValueResultSet.get(rs);
    }

    /**
     * Convert a value to the specified type.
     *
     * @param targetType the type of the returned value
     * @return the converted value
     */
    public Value convertTo(int targetType) {
        // converting NULL is done in ValueNull
        // converting BLOB to CLOB and vice versa is done in ValueLob
        if (getType() == targetType) {
            return this;
        }
        try {
            switch (targetType) {
            case BOOLEAN:
                return toBoolean();
            case BYTE:
                return toByte();
            case SHORT:
                return toShort();
            case INT:
                return toInt();
            case LONG:
                return toLong();
            case DECIMAL:
                return toDecimal();
            case DOUBLE:
                return toDouble();
            case FLOAT:
                return toFloat();
            case DATE:
                return toDate();
            case TIME:
                return toTime();
            case TIMESTAMP:
                return toTimestamp();
            case BYTES:
                return toBytes();
            case JAVA_OBJECT:
                return toJavaObject();
            case BLOB:
                return toBlob();
            case CLOB:
                return toClob();
            case UUID:
                return toUuid();
            case ARRAY:
                return toArray();
            case LIST:
                return toList();
            case SET:
                return toSet();
            case MAP:
                return toMap();
            case ENUM:
                return toEnum();
            case RESULT_SET:
                return toResultSet();
            case STRING:
                return ValueString.get(getString());
            case STRING_IGNORECASE:
                return ValueStringIgnoreCase.get(getString());
            case STRING_FIXED:
                return ValueStringFixed.get(getString());
            case NULL:
                return ValueNull.INSTANCE;
            default:
                throw DbException.getInternalError("type=" + targetType);
            }
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, e, getString());
        }
    }

    /**
     * Compare this value against another value given that the values are of the
     * same data type.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTypeSafe(Value v, CompareMode mode) {
        if (this == v) {
            return 0;
        } else if (this == ValueNull.INSTANCE) {
            return -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        return compareSecure(v, mode);
    }

    /**
     * Compare this value against another value using the specified compare mode.
     *
     * @param v the other value
     * @param mode the compare mode
     * @return 0 if both values are equal, -1 if the other value is smaller, and
     *         1 otherwise
     */
    public final int compareTo(Value v, CompareMode mode) {
        if (this == v) {
            return 0;
        }
        if (this == ValueNull.INSTANCE) {
            return v == ValueNull.INSTANCE ? 0 : -1;
        } else if (v == ValueNull.INSTANCE) {
            return 1;
        }
        if (getType() == v.getType()) {
            return compareSecure(v, mode);
        }
        int t2 = Value.getHigherOrder(getType(), v.getType());
        return convertTo(t2).compareSecure(v.convertTo(t2), mode);
    }

    public int getScale() {
        return 0;
    }

    /**
     * Convert the scale.
     *
     * @param onlyToSmallerScale if the scale should not reduced
     * @param targetScale the requested scale
     * @return the value
     */
    public Value convertScale(boolean onlyToSmallerScale, int targetScale) {
        return this;
    }

    /**
     * Convert the precision to the requested value. The precision of the
     * returned value may be somewhat larger than requested, because values with
     * a fixed precision are not truncated.
     *
     * @param precision the new precision
     * @param force true if losing numeric precision is allowed
     * @return the new value
     */
    public Value convertPrecision(long precision, boolean force) {
        return this;
    }

    private static byte convertToByte(long x) {
        if (x > Byte.MAX_VALUE || x < Byte.MIN_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(x));
        }
        return (byte) x;
    }

    private static short convertToShort(long x) {
        if (x > Short.MAX_VALUE || x < Short.MIN_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(x));
        }
        return (short) x;
    }

    private static int convertToInt(long x) {
        if (x > Integer.MAX_VALUE || x < Integer.MIN_VALUE) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Long.toString(x));
        }
        return (int) x;
    }

    private static long convertToLong(double x) {
        if (x > Long.MAX_VALUE || x < Long.MIN_VALUE) {
            // TODO document that +Infinity, -Infinity throw an exception and NaN returns 0
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, Double.toString(x));
        }
        return Math.round(x);
    }

    private static long convertToLong(BigDecimal x) {
        if (x.compareTo(MAX_LONG_DECIMAL) > 0 || x.compareTo(Value.MIN_LONG_DECIMAL) < 0) {
            throw DbException.get(ErrorCode.NUMERIC_VALUE_OUT_OF_RANGE_1, x.toString());
        }
        return x.setScale(0, RoundingMode.HALF_UP).longValue();
    }

    /**
     * Close the underlying resource, if any. For values that are kept fully in
     * memory this method has no effect.
     */
    public void close() {
        // nothing to do
    }

    /**
     * Check if the precision is smaller or equal than the given precision.
     *
     * @param precision the maximum precision
     * @return true if the precision of this value is smaller or equal to the
     *         given precision
     */
    public boolean checkPrecision(long precision) {
        return getPrecision() <= precision;
    }

    /**
     * Get a medium size SQL expression for debugging or tracing. If the precision is
     * too large, only a subset of the value is returned.
     *
     * @return the SQL expression
     */
    public String getTraceSQL() {
        return getSQL();
    }

    @Override
    public String toString() {
        return getTraceSQL();
    }

    /**
     * Throw the exception that the feature is not support for the given data type.
     *
     * @param op the operation
     * @return never returns normally
     * @throws DbException the exception
     */
    protected DbException throwUnsupportedExceptionForType(String op) {
        throw DbException.getUnsupportedException(DataType.getDataType(getType()).name + " " + op);
    }

    public ResultSet getResultSet() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("X", DataType.convertTypeToSQLType(getType()),
                MathUtils.convertLongToInt(getPrecision()), getScale());
        rs.addRow(getObject());
        return rs;
    }

    public final boolean isFalse() {
        return this != ValueNull.INSTANCE && !getBoolean();
    }

    @SuppressWarnings("unchecked")
    public <T> T getCollection() {
        return (T) getObject();
    }

    static int getCollectionComponentTypeFromClass(Class<?> clz) {
        if (clz == null || clz == Object.class)
            return Value.UNKNOWN; // 集合类型会自动根据元素构建相应的ValueXxx，所以不能用Value.JAVA_OBJECT
        else
            return DataType.getTypeFromClass(clz);
    }
}
