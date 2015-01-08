/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.local;

import java.nio.ByteBuffer;

import org.lealone.mvstore.DataUtils;
import org.lealone.mvstore.WriteBuffer;
import org.lealone.mvstore.type.DataType;

/**
 * The value type for a versioned value.
 */
class VersionedValueType implements DataType {

    private final DataType valueType;

    public VersionedValueType(DataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        VersionedValue v = (VersionedValue) obj;
        return valueType.getMemory(v.value) + 8;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        VersionedValue a = (VersionedValue) aObj;
        VersionedValue b = (VersionedValue) bObj;
        long comp = a.operationId - b.operationId;
        if (comp == 0) {
            return valueType.compare(a.value, b.value);
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        if (buff.get() == 0) {
            // fast path (no op ids or null entries)
            for (int i = 0; i < len; i++) {
                VersionedValue v = new VersionedValue();
                v.value = valueType.read(buff);
                obj[i] = v;
            }
        } else {
            // slow path (some entries may be null)
            for (int i = 0; i < len; i++) {
                obj[i] = read(buff);
            }
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        VersionedValue v = new VersionedValue();
        v.operationId = DataUtils.readVarLong(buff);
        if (buff.get() == 1) {
            v.value = valueType.read(buff);
        }
        return v;
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        boolean fastPath = true;
        for (int i = 0; i < len; i++) {
            VersionedValue v = (VersionedValue) obj[i];
            if (v.operationId != 0 || v.value == null) {
                fastPath = false;
            }
        }
        if (fastPath) {
            buff.put((byte) 0);
            for (int i = 0; i < len; i++) {
                VersionedValue v = (VersionedValue) obj[i];
                valueType.write(buff, v.value);
            }
        } else {
            // slow path:
            // store op ids, and some entries may be null
            buff.put((byte) 1);
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        VersionedValue v = (VersionedValue) obj;
        buff.putVarLong(v.operationId);
        if (v.value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, v.value);
        }
    }

}