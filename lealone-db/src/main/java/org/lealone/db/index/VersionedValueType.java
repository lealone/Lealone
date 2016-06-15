package org.lealone.db.index;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.value.ValueArray;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;

class VersionedValueType implements DataType {

    final DataType valueType;

    public VersionedValueType(DataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        VersionedValue v = (VersionedValue) obj;
        if (v == null)
            return 4;
        return valueType.getMemory(v.value) + 4;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        VersionedValue a = (VersionedValue) aObj;
        VersionedValue b = (VersionedValue) bObj;
        long comp = a.vertion - b.vertion;
        if (comp == 0) {
            return valueType.compare(a.value, b.value);
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        int vertion = DataUtils.readVarInt(buff);
        ValueArray value = (ValueArray) valueType.read(buff);
        return new VersionedValue(vertion, value);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarInt(v.vertion);
        valueType.write(buff, v.value);
    }
}
