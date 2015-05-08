/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.transaction2;

import java.nio.ByteBuffer;

import org.lealone.engine.DataType;
import org.lealone.engine.WriteBuffer;

/**
 * A data type that contains an array of objects with the specified data
 * types.
 */
class ArrayType implements DataType {

    private final int arrayLength;
    private final DataType[] elementTypes;

    ArrayType(DataType[] elementTypes) {
        this.arrayLength = elementTypes.length;
        this.elementTypes = elementTypes;
    }

    @Override
    public int getMemory(Object obj) {
        Object[] array = (Object[]) obj;
        int size = 0;
        for (int i = 0; i < arrayLength; i++) {
            DataType t = elementTypes[i];
            Object o = array[i];
            if (o != null) {
                size += t.getMemory(o);
            }
        }
        return size;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        if (aObj == bObj) {
            return 0;
        }
        Object[] a = (Object[]) aObj;
        Object[] b = (Object[]) bObj;
        for (int i = 0; i < arrayLength; i++) {
            DataType t = elementTypes[i];
            int comp = t.compare(a[i], b[i]);
            if (comp != 0) {
                return comp;
            }
        }
        return 0;
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        Object[] array = (Object[]) obj;
        for (int i = 0; i < arrayLength; i++) {
            DataType t = elementTypes[i];
            Object o = array[i];
            if (o == null) {
                buff.put((byte) 0);
            } else {
                buff.put((byte) 1);
                t.write(buff, o);
            }
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        Object[] array = new Object[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            DataType t = elementTypes[i];
            if (buff.get() == 1) {
                array[i] = t.read(buff);
            }
        }
        return array;
    }

}