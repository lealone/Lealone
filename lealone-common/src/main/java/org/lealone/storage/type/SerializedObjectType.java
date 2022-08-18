/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;

public class SerializedObjectType extends StorageDataTypeBase {

    private int averageSize = 10000;
    private final ObjectDataType base = new ObjectDataType();

    @Override
    public int getType() {
        return TYPE_SERIALIZED_OBJECT;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        StorageDataType ta = getType(aObj);
        StorageDataType tb = getType(bObj);
        if (ta.getClass() != this.getClass() || tb.getClass() != this.getClass()) {
            if (ta == tb) {
                return ta.compare(aObj, bObj);
            }
        }
        // TODO ensure comparable type (both may be comparable but not
        // with each other)
        if (aObj instanceof Comparable) {
            if (aObj.getClass().isAssignableFrom(bObj.getClass())) {
                return ((Comparable<Object>) aObj).compareTo(bObj);
            }
        }
        if (bObj instanceof Comparable) {
            if (bObj.getClass().isAssignableFrom(aObj.getClass())) {
                return -((Comparable<Object>) bObj).compareTo(aObj);
            }
        }
        byte[] a = serialize(aObj);
        byte[] b = serialize(bObj);
        return ObjectDataType.compareNotNull(a, b);
    }

    private StorageDataType getType(Object obj) {
        return base.switchType(obj);
    }

    @Override
    public int getMemory(Object obj) {
        StorageDataType t = getType(obj);
        if (t.getClass() == this.getClass()) {
            return averageSize;
        }
        return t.getMemory(obj);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        StorageDataType t = getType(obj);
        if (t != this) {
            t.write(buff, obj);
            return;
        }
        byte[] data = serialize(obj);
        // we say they are larger, because these objects
        // use quite a lot of disk space
        int size = data.length * 2;
        // adjust the average size
        // using an exponential moving average
        averageSize = (size + 15 * averageSize) / 16;
        buff.put((byte) TYPE_SERIALIZED_OBJECT).putVarInt(data.length).put(data);
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        int len = DataUtils.readVarInt(buff);
        byte[] data = DataUtils.newBytes(len);
        buff.get(data);
        return deserialize(data);
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

    /**
     * Serialize the object to a byte array.
     *
     * @param obj the object to serialize
     * @return the byte array
     */
    private static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(out);
            os.writeObject(obj);
            return out.toByteArray();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not serialize {0}", obj, e);
        }
    }

    /**
     * De-serialize the byte array to an object.
     *
     * @param data the byte array
     * @return the object
     */
    private static Object deserialize(byte[] data) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ObjectInputStream is = new ObjectInputStream(in);
            return is.readObject();
        } catch (Throwable e) {
            throw DataUtils.newIllegalArgumentException("Could not deserialize {0}",
                    Arrays.toString(data), e);
        }
    }

}
