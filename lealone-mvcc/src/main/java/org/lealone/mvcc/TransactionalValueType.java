/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.mvcc;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;

/**
 * The value type for a transactional value.
 * 
 * @author H2 Group
 * @author zhh
 */
class TransactionalValueType implements DataType {

    final DataType valueType;

    public TransactionalValueType(DataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        return valueType.getMemory(v.value) + 12;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        TransactionalValue a = (TransactionalValue) aObj;
        TransactionalValue b = (TransactionalValue) bObj;
        long comp = a.tid - b.tid;
        if (comp == 0) {
            comp = a.logId - b.logId;
            if (comp == 0)
                return valueType.compare(a.value, b.value);
        }
        return Long.signum(comp);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        if (buff.get() == 0) {
            // fast path (no tid/logId or null entries)
            for (int i = 0; i < len; i++) {
                obj[i] = new TransactionalValue(valueType.read(buff));
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
        long tid = DataUtils.readVarLong(buff);
        int logId = DataUtils.readVarInt(buff);
        Object value = null;
        if (buff.get() == 1) {
            value = valueType.read(buff);
        }
        return new TransactionalValue(tid, logId, value);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        boolean fastPath = true;
        for (int i = 0; i < len; i++) {
            TransactionalValue v = (TransactionalValue) obj[i];
            if (v.tid != 0 || v.value == null) {
                fastPath = false;
            }
        }
        if (fastPath) {
            buff.put((byte) 0);
            for (int i = 0; i < len; i++) {
                TransactionalValue v = (TransactionalValue) obj[i];
                valueType.write(buff, v.value);
            }
        } else {
            // slow path:
            // store tid/logId, and some entries may be null
            buff.put((byte) 1);
            for (int i = 0; i < len; i++) {
                write(buff, obj[i]);
            }
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        buff.putVarLong(v.tid);
        buff.putVarInt(v.logId);
        if (v.value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, v.value);
        }
    }
}
