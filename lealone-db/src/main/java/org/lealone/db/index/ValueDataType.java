/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.lealone.db.DataBuffer;
import org.lealone.db.DataHandler;
import org.lealone.db.result.SortOrder;
import org.lealone.db.value.CompareMode;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.storage.type.StorageDataType;

/**
 * A row type.
 */
public class ValueDataType implements StorageDataType {

    final DataHandler handler;
    final CompareMode compareMode;
    final int[] sortTypes;

    public ValueDataType(DataHandler handler, CompareMode compareMode, int[] sortTypes) {
        this.handler = handler;
        this.compareMode = compareMode;
        this.sortTypes = sortTypes;
    }

    @Override
    public int compare(Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a instanceof ValueArray && b instanceof ValueArray) {
            Value[] ax = ((ValueArray) a).getList();
            Value[] bx = ((ValueArray) b).getList();
            int al = ax.length;
            int bl = bx.length;
            int len = Math.min(al, bl);
            for (int i = 0; i < len; i++) {
                int sortType = sortTypes[i];
                int comp = compareValues(ax[i], bx[i], sortType);
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
        return compareValues((Value) a, (Value) b, SortOrder.ASCENDING);
    }

    private int compareValues(Value a, Value b, int sortType) {
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
    public void read(ByteBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(DataBuffer buff, Object[] obj, int len) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        return DataBuffer.readValue(buff);
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
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
        } else if (!(obj instanceof ValueDataType)) {
            return false;
        }
        ValueDataType v = (ValueDataType) obj;
        if (!compareMode.equals(v.compareMode)) {
            return false;
        }
        return Arrays.equals(sortTypes, v.sortTypes);
    }

}
