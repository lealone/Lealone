/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DataBuffer;

/**
 * Implementation of NULL. NULL is not a regular data type.
 */
public class ValueNull extends Value {

    /**
     * The main NULL instance.
     */
    public static final ValueNull INSTANCE = new ValueNull();

    /**
     * This special instance is used as a marker for deleted entries in a map.
     * It should not be used anywhere else.
     */
    public static final ValueNull DELETED = new ValueNull();

    /**
     * The precision of NULL.
     */
    private static final int PRECISION = 1;

    /**
     * The display size of the textual representation of NULL.
     */
    private static final int DISPLAY_SIZE = 4;

    private ValueNull() {
        // don't allow construction
    }

    @Override
    public String getSQL() {
        return "NULL";
    }

    @Override
    public int getType() {
        return Value.NULL;
    }

    @Override
    public String getString() {
        return null;
    }

    @Override
    public boolean getBoolean() {
        return false;
    }

    @Override
    public Date getDate() {
        return null;
    }

    @Override
    public Time getTime() {
        return null;
    }

    @Override
    public Timestamp getTimestamp() {
        return null;
    }

    @Override
    public byte[] getBytes() {
        return null;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public short getShort() {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal() {
        return null;
    }

    @Override
    public double getDouble() {
        return 0.0;
    }

    @Override
    public float getFloat() {
        return 0.0F;
    }

    @Override
    public int getInt() {
        return 0;
    }

    @Override
    public long getLong() {
        return 0;
    }

    @Override
    public InputStream getInputStream() {
        return null;
    }

    @Override
    public Reader getReader() {
        return null;
    }

    @Override
    public Value convertTo(int type) {
        return this;
    }

    @Override
    protected int compareSecure(Value v, CompareMode mode) {
        throw DbException.getInternalError("compare null");
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Object getObject() {
        return null;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setNull(parameterIndex, DataType.convertTypeToSQLType(Value.NULL));
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    public boolean equals(Object other) {
        return other == this;
    }

    @Override
    public int getMemory() {
        return 0; // 总是在内存中
    }

    public static final ValueDataTypeBase type = new ValueDataTypeBase() {

        @Override
        public int getType() {
            return NULL;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            if (aObj == null && bObj == null) {
                return 0;
            } else if (aObj == null) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public int getMemory(Object obj) {
            return 0;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            buff.put((byte) Value.NULL);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            buff.put((byte) Value.NULL);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return ValueNull.INSTANCE;
        }
    };
}
