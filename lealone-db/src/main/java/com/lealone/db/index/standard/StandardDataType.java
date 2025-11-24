/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.index.standard;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataHandler;
import com.lealone.db.result.SortOrder;
import com.lealone.db.value.CompareMode;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.storage.type.StorageDataTypeBase;

public class StandardDataType extends StorageDataTypeBase {

    final DataHandler handler;
    final CompareMode compareMode;
    final int[] sortTypes;

    public StandardDataType(DataHandler handler, CompareMode compareMode, int[] sortTypes) {
        this.handler = handler;
        this.compareMode = compareMode;
        this.sortTypes = sortTypes;
    }

    protected boolean isUniqueKey() {
        return false;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof ValueArray && b instanceof ValueArray) {
            Value[] ax = ((ValueArray) a).getList();
            Value[] bx = ((ValueArray) b).getList();
            return compareValues(ax, bx);
        }
        return compareValue((Value) a, (Value) b, SortOrder.ASCENDING);
    }

    public int compareValues(Value[] ax, Value[] bx) {
        int al = ax.length;
        int bl = bx.length;
        int len = Math.min(al, bl);
        // 唯一索引key不需要比较最后的rowId
        int size = isUniqueKey() ? len - 1 : len;
        for (int i = 0; i < size; i++) {
            int sortType = sortTypes[i];
            int comp = compareValue(ax[i], bx[i], sortType);
            if (comp != 0) {
                return comp;
            }
        }
        if (len < al) {
            return -1;
        } else if (len < bl) {
            return 1;
        }
        return 0;
    }

    private int compareValue(Value a, Value b, int sortType) {
        if (a == b) {
            return 0;
        }
        // null is never stored;
        // comparison with null is used to retrieve all entries
        // in which case null is always lower than all entries
        // (even for descending ordered indexes)
        if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }
        boolean aNull = a == ValueNull.INSTANCE;
        boolean bNull = b == ValueNull.INSTANCE;
        if (aNull || bNull) {
            return SortOrder.compareNull(aNull, sortType);
        }
        int comp = compareTypeSafe(a, b);
        if ((sortType & SortOrder.DESCENDING) != 0) {
            comp = -comp;
        }
        return comp;
    }

    private int compareTypeSafe(Value a, Value b) {
        if (a == b) {
            return 0;
        }
        return a.compareTypeSafe(b, compareMode);
    }

    @Override
    public int getMemory(Object obj) {
        return getMemory((Value) obj);
    }

    private static int getMemory(Value v) {
        return v == null ? 0 : v.getMemory();
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, int formatVersion) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff, formatVersion);
        }
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len, int formatVersion) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i], formatVersion);
        }
    }

    @Override
    public Object read(ByteBuffer buff, int formatVersion) {
        return DataBuffer.readValue(buff);
    }

    @Override
    public void write(DataBuffer buff, Object obj, int formatVersion) {
        Value x = (Value) obj;
        buff.writeValue(x);
    }

    @Override
    public int hashCode() {
        return compareMode.hashCode() ^ Arrays.hashCode(sortTypes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (!(obj instanceof StandardDataType)) {
            return false;
        }
        StandardDataType v = (StandardDataType) obj;
        if (!compareMode.equals(v.compareMode)) {
            return false;
        }
        return Arrays.equals(sortTypes, v.sortTypes);
    }

    private boolean keyOnly;

    @Override
    public boolean isKeyOnly() {
        return keyOnly;
    }

    public void setKeyOnly(boolean keyOnly) {
        this.keyOnly = keyOnly;
    }

    @Override
    public Object getAppendKey(long key, Object valueObj) {
        return ValueLong.get(key);
    }
}
