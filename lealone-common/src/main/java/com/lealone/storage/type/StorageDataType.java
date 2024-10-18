/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.type;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;

/**
 * A data type.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface StorageDataType {

    /**
     * The type constants are also used as tag values.
     */
    public static final int TYPE_NULL = Value.NULL;
    public static final int TYPE_BOOLEAN = Value.BOOLEAN;
    public static final int TYPE_BYTE = Value.BYTE;
    public static final int TYPE_SHORT = Value.SHORT;
    public static final int TYPE_INT = Value.INT;
    public static final int TYPE_LONG = Value.LONG;
    public static final int TYPE_FLOAT = Value.FLOAT;
    public static final int TYPE_DOUBLE = Value.DOUBLE;
    public static final int TYPE_BIG_DECIMAL = Value.DECIMAL;
    public static final int TYPE_STRING = Value.STRING;
    public static final int TYPE_UUID = Value.UUID;
    public static final int TYPE_DATE = Value.DATE;
    public static final int TYPE_TIME = Value.TIME;
    public static final int TYPE_TIMESTAMP = Value.TIMESTAMP;

    public static final int TYPE_BIG_INTEGER = 25;
    public static final int TYPE_CHAR = 26;
    public static final int TYPE_ARRAY = 27;
    public static final int TYPE_SERIALIZED_OBJECT = 28;

    /**
     * For very common values (e.g. 0 and 1) we save space by encoding the value
     * in the tag. e.g. TAG_BOOLEAN_TRUE and TAG_FLOAT_0.
     */
    public static final int TAG_BOOLEAN_TRUE = 32;
    public static final int TAG_INTEGER_NEGATIVE = 33;
    public static final int TAG_INTEGER_FIXED = 34;
    public static final int TAG_LONG_NEGATIVE = 35;
    public static final int TAG_LONG_FIXED = 36;
    public static final int TAG_BIG_INTEGER_0 = 37;
    public static final int TAG_BIG_INTEGER_1 = 38;
    public static final int TAG_BIG_INTEGER_SMALL = 39;
    public static final int TAG_FLOAT_0 = 40;
    public static final int TAG_FLOAT_1 = 41;
    public static final int TAG_FLOAT_FIXED = 42;
    public static final int TAG_DOUBLE_0 = 43;
    public static final int TAG_DOUBLE_1 = 44;
    public static final int TAG_DOUBLE_FIXED = 45;
    public static final int TAG_BIG_DECIMAL_0 = 46;
    public static final int TAG_BIG_DECIMAL_1 = 47;
    public static final int TAG_BIG_DECIMAL_SMALL = 48;
    public static final int TAG_BIG_DECIMAL_SMALL_SCALED = 49;

    /**
     * For small-values/small-arrays, we encode the value/array-length in the tag.
     */
    public static final int TAG_INTEGER_0_15 = 64;
    public static final int TAG_LONG_0_7 = 80;
    public static final int TAG_STRING_0_15 = 88;
    public static final int TAG_BYTE_ARRAY_0_15 = 104;

    public static final int FLOAT_ZERO_BITS = Float.floatToIntBits(0.0f);
    public static final int FLOAT_ONE_BITS = Float.floatToIntBits(1.0f);
    public static final long DOUBLE_ZERO_BITS = Double.doubleToLongBits(0.0d);
    public static final long DOUBLE_ONE_BITS = Double.doubleToLongBits(1.0d);

    /**
     * Compare two keys.
     *
     * @param aObj the first key
     * @param bObj the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     * @throws UnsupportedOperationException if the type is not orderable
     */
    int compare(Object aObj, Object bObj);

    /**
     * Estimate the used memory in bytes.
     *
     * @param obj the object
     * @return the used memory
     */
    int getMemory(Object obj);

    /**
     * Write an object.
     *
     * @param buff the target buffer
     * @param obj the value
     */
    void write(DataBuffer buff, Object obj);

    /**
     * Write a list of objects.
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to write
     */
    default void write(DataBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    /**
     * Read an object.
     *
     * @param buff the source buffer
     * @return the object
     */
    Object read(ByteBuffer buff);

    /**
     * Read a list of objects.
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to read
     */
    default void read(ByteBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    default void writeMeta(DataBuffer buff, Object obj) {
        // do nothing
    }

    default Object readMeta(ByteBuffer buff, int columnCount) {
        // do nothing
        return null;
    }

    default void writeColumn(DataBuffer buff, Object obj, int columnIndex) {
        write(buff, obj);
    }

    default void readColumn(ByteBuffer buff, Object obj, int columnIndex) {
        // do nothing
    }

    default void setColumns(Object oldObj, Object newObj, int[] columnIndexes) {
        // do nothing
    }

    default ValueArray getColumns(Object obj) {
        return null;
    }

    default int getColumnCount() {
        return 1;
    }

    default int getMemory(Object obj, int columnIndex) {
        return getMemory(obj);
    }

    default Object convertToIndexKey(Object key, Object value) {
        return null;
    }

    public static int getTypeId(int tag) {
        int typeId;
        if (tag <= TYPE_SERIALIZED_OBJECT) {
            typeId = tag;
        } else {
            switch (tag) {
            case TAG_BOOLEAN_TRUE:
                typeId = TYPE_BOOLEAN;
                break;
            case TAG_INTEGER_NEGATIVE:
            case TAG_INTEGER_FIXED:
                typeId = TYPE_INT;
                break;
            case TAG_LONG_NEGATIVE:
            case TAG_LONG_FIXED:
                typeId = TYPE_LONG;
                break;
            case TAG_BIG_INTEGER_0:
            case TAG_BIG_INTEGER_1:
            case TAG_BIG_INTEGER_SMALL:
                typeId = TYPE_BIG_INTEGER;
                break;
            case TAG_FLOAT_0:
            case TAG_FLOAT_1:
            case TAG_FLOAT_FIXED:
                typeId = TYPE_FLOAT;
                break;
            case TAG_DOUBLE_0:
            case TAG_DOUBLE_1:
            case TAG_DOUBLE_FIXED:
                typeId = TYPE_DOUBLE;
                break;
            case TAG_BIG_DECIMAL_0:
            case TAG_BIG_DECIMAL_1:
            case TAG_BIG_DECIMAL_SMALL:
            case TAG_BIG_DECIMAL_SMALL_SCALED:
                typeId = TYPE_BIG_DECIMAL;
                break;
            default:
                if (tag >= TAG_INTEGER_0_15 && tag <= TAG_INTEGER_0_15 + 15) {
                    typeId = TYPE_INT;
                } else if (tag >= TAG_STRING_0_15 && tag <= TAG_STRING_0_15 + 15) {
                    typeId = TYPE_STRING;
                } else if (tag >= TAG_LONG_0_7 && tag <= TAG_LONG_0_7 + 7) {
                    typeId = TYPE_LONG;
                } else if (tag >= TAG_BYTE_ARRAY_0_15 && tag <= TAG_BYTE_ARRAY_0_15 + 15) {
                    typeId = TYPE_ARRAY;
                } else {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                            "Unknown tag {0}", tag);
                }
            }
        }
        return typeId;
    }
}
