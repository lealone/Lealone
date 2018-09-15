/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction.mvcc;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.type.StorageDataType;

/**
 * The value type for a transactional value.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TransactionalValueType implements StorageDataType {

    public final StorageDataType valueType;

    public TransactionalValueType(StorageDataType valueType) {
        this.valueType = valueType;
    }

    @Override
    public int getMemory(Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        return valueType.getMemory(v.value);
        // TODO 由于BufferedMap的合并与复制逻辑的验证是并行的，
        // 可能导致split时三个复制节点中某些相同的TransactionalValue有些globalReplicationName为null，有些不为null
        // 这样就会得到不同的内存大小，从而使得splitKey不同
        // return valueType.getMemory(v.value) + 12
        // + (v.globalReplicationName == null ? 1 : 9 + ValueString.type.getMemory(v.globalReplicationName));
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
    public void read(ByteBuffer buff, Object[] obj, int len) {
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
        TransactionalValue transactionalValue = null;
        if (buff.get() == 1) {
            long version = DataUtils.readVarLong(buff);
            String globalTransactionName = ValueString.type.read(buff);
            transactionalValue = new TransactionalValue(tid, logId, value, version, globalTransactionName);
        } else {
            transactionalValue = new TransactionalValue(tid, logId, value);
        }

        if (buff.get() == 1) {
            transactionalValue.hostAndPort = ValueString.type.read(buff);
        }
        return transactionalValue;
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len) {
        boolean fastPath = true;
        for (int i = 0; i < len; i++) {
            TransactionalValue v = (TransactionalValue) obj[i];
            if (v.tid != 0 || v.value == null || v.globalReplicationName != null) {
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
    public void write(DataBuffer buff, Object obj) {
        TransactionalValue v = (TransactionalValue) obj;
        buff.putVarLong(v.tid);
        buff.putVarInt(v.logId);
        if (v.value == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            valueType.write(buff, v.value);
        }
        if (v.globalReplicationName == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            buff.putVarLong(v.version);
            ValueString.type.write(buff, v.globalReplicationName);
        }
        if (v.hostAndPort == null) {
            buff.put((byte) 0);
        } else {
            buff.put((byte) 1);
            ValueString.type.write(buff, v.hostAndPort);
        }
    }
}
