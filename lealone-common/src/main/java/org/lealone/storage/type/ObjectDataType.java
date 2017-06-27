/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

import org.lealone.common.util.DataUtils;
import org.lealone.common.util.New;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueByte;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueDecimal;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueFloat;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueShort;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueTime;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.db.value.ValueUuid;

/**
 * A data type implementation for the most common data types, including
 * serializable objects.
 */
public class ObjectDataType implements StorageDataType {

    static final Class<?>[] COMMON_CLASSES = { boolean.class, byte.class, short.class, char.class, int.class,
            long.class, float.class, double.class, Object.class, Boolean.class, Byte.class, Short.class,
            Character.class, Integer.class, Long.class, BigInteger.class, Float.class, Double.class, BigDecimal.class,
            String.class, UUID.class, Date.class, Time.class, Timestamp.class };

    private static final HashMap<Class<?>, Integer> COMMON_CLASSES_MAP = New.hashMap();

    private StorageDataTypeBase last = ValueString.type;

    @Override
    public int compare(Object a, Object b) {
        switchType(a);
        return last.compare(a, b);
    }

    @Override
    public int getMemory(Object obj) {
        switchType(obj);
        return last.getMemory(obj);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        switchType(obj);
        last.write(buff, obj);
    }

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        int typeId = StorageDataType.getTypeId(tag);
        StorageDataTypeBase t = last;
        if (typeId != t.getType()) {
            last = t = newType(typeId);
        }
        return t.read(buff, tag);
    }

    private StorageDataTypeBase newType(int typeId) {
        switch (typeId) {
        case TYPE_NULL:
            return ValueNull.type;
        case TYPE_BOOLEAN:
            return ValueBoolean.type;
        case TYPE_BYTE:
            return ValueByte.type;
        case TYPE_SHORT:
            return ValueShort.type;
        case TYPE_CHAR:
            return new CharacterType();
        case TYPE_INT:
            return ValueInt.type;
        case TYPE_LONG:
            return ValueLong.type;
        case TYPE_FLOAT:
            return ValueFloat.type;
        case TYPE_DOUBLE:
            return ValueDouble.type;
        case TYPE_BIG_INTEGER:
            return new BigIntegerType();
        case TYPE_BIG_DECIMAL:
            return ValueDecimal.type;
        case TYPE_STRING:
            return ValueString.type;
        case TYPE_UUID:
            return ValueUuid.type;
        case TYPE_DATE:
            return ValueDate.type;
        case TYPE_TIME:
            return ValueTime.type;
        case TYPE_TIMESTAMP:
            return ValueTimestamp.type;
        case TYPE_ARRAY:
            return new ObjectArrayType();
        case TYPE_SERIALIZED_OBJECT:
            return new SerializedObjectType();
        }
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}", typeId);
    }

    /**
    * Switch the last remembered type to match the type of the given object.
    *
    * @param obj the object
    * @return the auto-detected type used
    */
    StorageDataTypeBase switchType(Object obj) {
        int typeId = getTypeId(obj);
        StorageDataTypeBase l = last;
        if (typeId != l.getType()) {
            last = l = newType(typeId);
        }
        return l;
    }

    private static int getTypeId(Object obj) {
        if (obj instanceof Integer) {
            return TYPE_INT;
        } else if (obj instanceof String) {
            return TYPE_STRING;
        } else if (obj instanceof Long) {
            return TYPE_LONG;
        } else if (obj instanceof Double) {
            return TYPE_DOUBLE;
        } else if (obj instanceof Float) {
            return TYPE_FLOAT;
        } else if (obj instanceof Boolean) {
            return TYPE_BOOLEAN;
        } else if (obj instanceof UUID) {
            return TYPE_UUID;
        } else if (obj instanceof Byte) {
            return TYPE_BYTE;
        } else if (obj instanceof Short) {
            return TYPE_SHORT;
        } else if (obj instanceof Character) {
            return TYPE_CHAR;
        } else if (obj == null) {
            return TYPE_NULL;
        } else if (isDate(obj)) {
            return TYPE_DATE;
        } else if (isTime(obj)) {
            return TYPE_TIME;
        } else if (isTimestamp(obj)) {
            return TYPE_TIMESTAMP;
        } else if (isBigInteger(obj)) {
            return TYPE_BIG_INTEGER;
        } else if (isBigDecimal(obj)) {
            return TYPE_BIG_DECIMAL;
        } else if (obj.getClass().isArray()) {
            return TYPE_ARRAY;
        }
        return TYPE_SERIALIZED_OBJECT;
    }

    /**
     * Check whether this object is a BigInteger.
     *
     * @param obj the object
     * @return true if yes
     */
    private static boolean isBigInteger(Object obj) {
        return obj instanceof BigInteger && obj.getClass() == BigInteger.class;
    }

    /**
     * Check whether this object is a BigDecimal.
     *
     * @param obj the object
     * @return true if yes
     */
    private static boolean isBigDecimal(Object obj) {
        return obj instanceof BigDecimal && obj.getClass() == BigDecimal.class;
    }

    /**
     * Check whether this object is a date.
     *
     * @param obj the object
     * @return true if yes
     */
    private static boolean isDate(Object obj) {
        return obj instanceof Date && obj.getClass() == Date.class;
    }

    private static boolean isTime(Object obj) {
        return obj instanceof Time && obj.getClass() == Time.class;
    }

    private static boolean isTimestamp(Object obj) {
        return obj instanceof Timestamp && obj.getClass() == Timestamp.class;
    }

    // /**
    // * Check whether this object is an array.
    // *
    // * @param obj the object
    // * @return true if yes
    // */
    // private static boolean isArray(Object obj) {
    // return obj != null && obj.getClass().isArray();
    // }

    /**
     * Get the class id, or null if not found.
     *
     * @param clazz the class
     * @return the class id or null
     */
    static Integer getCommonClassId(Class<?> clazz) {
        HashMap<Class<?>, Integer> map = COMMON_CLASSES_MAP;
        if (map.isEmpty()) {
            // lazy initialization
            for (int i = 0, size = COMMON_CLASSES.length; i < size; i++) {
                COMMON_CLASSES_MAP.put(COMMON_CLASSES[i], i);
            }
        }
        return map.get(clazz);
    }

    /**
     * Compare the contents of two byte arrays. If the content or length of the
     * first array is smaller than the second array, -1 is returned. If the
     * content or length of the second array is smaller than the first array, 1
     * is returned. If the contents and lengths are the same, 0 is returned.
     * <p>
     * This method interprets bytes as unsigned.
     *
     * @param data1 the first byte array (must not be null)
     * @param data2 the second byte array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    static int compareNotNull(byte[] data1, byte[] data2) {
        if (data1 == data2) {
            return 0;
        }
        int len = Math.min(data1.length, data2.length);
        for (int i = 0; i < len; i++) {
            int b = data1[i] & 255;
            int b2 = data2[i] & 255;
            if (b != b2) {
                return b > b2 ? 1 : -1;
            }
        }
        return Integer.signum(data1.length - data2.length);
    }

}
