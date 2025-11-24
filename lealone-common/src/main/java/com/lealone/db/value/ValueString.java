/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.value;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.lealone.common.util.DataUtils;
import com.lealone.common.util.MathUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.SysProperties;
import com.lealone.storage.FormatVersion;

/**
 * Implementation of the VARCHAR data type.
 * It is also the base class for other ValueString* classes.
 */
public class ValueString extends Value {

    private static final ValueString EMPTY = new ValueString("");

    /**
     * The string data.
     */
    protected final String value;

    protected ValueString(String value) {
        this.value = value;
    }

    @Override
    public String getSQL() {
        return StringUtils.quoteStringSQL(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ValueString && value.equals(((ValueString) other).value);
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        // compatibility: the other object could be another type
        ValueString v = (ValueString) o;
        return mode.compareString(value, v.value, false);
    }

    @Override
    public String getString() {
        return value;
    }

    @Override
    public long getPrecision() {
        return value.length();
    }

    @Override
    public Object getObject() {
        return value;
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setString(parameterIndex, value);
    }

    @Override
    public int getDisplaySize() {
        return value.length();
    }

    @Override
    public Value convertPrecision(long precision, boolean force) {
        if (precision == 0 || value.length() <= precision) {
            return this;
        }
        int p = MathUtils.convertLongToInt(precision);
        return getNew(value.substring(0, p));
    }

    @Override
    public int hashCode() {
        // TODO hash performance: could build a quicker hash
        // by hashing the size and a few characters
        return value.hashCode();

        // proposed code:
        // private int hash = 0;
        //
        // public int hashCode() {
        // int h = hash;
        // if (h == 0) {
        // String s = value;
        // int l = s.length();
        // if (l > 0) {
        // if (l < 16)
        // h = s.hashCode();
        // else {
        // h = l;
        // for (int i = 1; i <= l; i <<= 1)
        // h = 31 *
        // (31 * h + s.charAt(i - 1)) +
        // s.charAt(l - i);
        // }
        // hash = h;
        // }
        // }
        // return h;
        // }

    }

    @Override
    public int getType() {
        return Value.STRING;
    }

    /**
     * Get or create a string value for the given string.
     *
     * @param s the string
     * @return the value
     */
    public static ValueString get(String s) {
        if (s.length() == 0) {
            return EMPTY;
        }
        ValueString obj = new ValueString(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return (ValueString) Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

    /**
     * Get or create a string value for the given string.
     *
     * @param s the string
     * @param treatEmptyStringsAsNull whether or not to treat empty strings as
     *            NULL
     * @return the value
     */
    public static Value get(String s, boolean treatEmptyStringsAsNull) {
        if (s.isEmpty()) {
            return treatEmptyStringsAsNull ? ValueNull.INSTANCE : EMPTY;
        }
        ValueString obj = new ValueString(StringUtils.cache(s));
        if (s.length() > SysProperties.OBJECT_CACHE_MAX_PER_ELEMENT_SIZE) {
            return obj;
        }
        return Value.cache(obj);
        // this saves memory, but is really slow
        // return new ValueString(s.intern());
    }

    /**
     * Create a new String value of the current class.
     * This method is meant to be overridden by subclasses.
     *
     * @param s the string
     * @return the value
     */
    protected ValueString getNew(String s) {
        return ValueString.get(s);
    }

    @Override
    public int getMemory() {
        return 16 + 2 * value.length();
    }

    public static final StringDataType type = new StringDataType();

    public static final class StringDataType extends ValueDataTypeBase {

        private StringDataType() {
        }

        @Override
        public int getType() {
            return STRING;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            return aObj.toString().compareTo(bObj.toString());
        }

        @Override
        public int getMemory(Object obj) {
            return 16 + 2 * obj.toString().length();
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            String s = (String) obj;
            write0(buff, s);
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            String s = v.getString();
            write0(buff, s);
        }

        private void write0(DataBuffer buff, String s) {
            if (s == null) {
                buff.put((byte) STRING).putVarInt((byte) 0);
                return;
            }
            int len = s.length();
            if (len <= 15) {
                buff.put((byte) (TAG_STRING_0_15 + len));
            } else {
                buff.put((byte) STRING).putVarInt(len);
            }
            buff.putStringData(s, len);
        }

        public String read(ByteBuffer buff) {
            return read(buff, FormatVersion.FORMAT_VERSION_1);
        }

        @Override
        public String read(ByteBuffer buff, int formatVersion) {
            return (String) super.read(buff, formatVersion);
        }

        @Override
        public Value readValue(ByteBuffer buff, int tag) {
            int len;
            if (tag == STRING) {
                len = DataUtils.readVarInt(buff);
                if (len == 0)
                    return ValueNull.INSTANCE;
            } else {
                len = tag - TAG_STRING_0_15;
            }
            return ValueString.get(DataUtils.readString(buff, len));
        }
    };
}
