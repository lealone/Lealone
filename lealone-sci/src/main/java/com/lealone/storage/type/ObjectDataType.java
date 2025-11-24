/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.storage.type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueBoolean;
import com.lealone.db.value.ValueByte;
import com.lealone.db.value.ValueDataType;
import com.lealone.db.value.ValueDataTypeBase;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueDecimal;
import com.lealone.db.value.ValueDouble;
import com.lealone.db.value.ValueFloat;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueShort;
import com.lealone.db.value.ValueString;
import com.lealone.db.value.ValueTime;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.db.value.ValueUuid;

/**
 * A data type implementation for the most common data types, including serializable objects.
 * 
 * @author H2 Group
 * @author zhh
 */
// 这个类是单独使用存储引擎时，如果没未指定key和value的编解码类型，就用它自动对各种java对象进行编解码
// 而StandardDataType是把记录的字段按指定类型进行编解码，跟ObjectDataType保存的数据不一样
public class ObjectDataType extends ValueDataTypeBase {

    private ValueDataTypeBase last = ValueString.type;

    @Override
    public int getType() {
        return last.getType();
    }

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
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        switchType(obj);
        last.write(buff, obj, formatVersion);
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        int tag = buff.get();
        int typeId = ValueDataType.getTypeId(tag);
        return switchType(typeId).read(buff, tag, formatVersion);
    }

    @Override
    public Value readValue(ByteBuffer buff, int tag) {
        int typeId = ValueDataType.getTypeId(tag);
        return switchType(typeId).readValue(buff, tag);
    }

    private ValueDataTypeBase switchType(int typeId) {
        ValueDataTypeBase l = last;
        if (typeId != l.getType()) {
            last = l = newType(typeId);
        }
        return l;
    }

    /**
    * Switch the last remembered type to match the type of the given object.
    *
    * @param obj the object
    * @return the auto-detected type used
    */
    ValueDataTypeBase switchType(Object obj) {
        int typeId = getTypeId(obj);
        return switchType(typeId);
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

    private static boolean isBigInteger(Object obj) {
        return obj instanceof BigInteger && obj.getClass() == BigInteger.class;
    }

    private static boolean isBigDecimal(Object obj) {
        return obj instanceof BigDecimal && obj.getClass() == BigDecimal.class;
    }

    private static boolean isDate(Object obj) {
        return obj instanceof Date && obj.getClass() == Date.class;
    }

    private static boolean isTime(Object obj) {
        return obj instanceof Time && obj.getClass() == Time.class;
    }

    private static boolean isTimestamp(Object obj) {
        return obj instanceof Timestamp && obj.getClass() == Timestamp.class;
    }

    private static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }

    public static ValueDataTypeBase newType(int typeId) {
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
        throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Unsupported type {0}",
                typeId);
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
        } else if (isArray(obj)) {
            return TYPE_ARRAY;
        }
        return TYPE_SERIALIZED_OBJECT;
    }
}
