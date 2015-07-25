/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.type;

import java.nio.ByteBuffer;

import org.lealone.util.DataUtils;

/**
 * A string type.
 */
public class StringDataType implements DataType {

    public static final StringDataType INSTANCE = new StringDataType();

    protected StringDataType() {
        // 只允许子类继承
    }

    @Override
    public int compare(Object a, Object b) {
        return a.toString().compareTo(b.toString());
    }

    @Override
    public int getMemory(Object obj) {
        if (obj == null)
            return 4;
        return 24 + 2 * obj.toString().length();
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
    public String read(ByteBuffer buff) {
        int len = DataUtils.readVarInt(buff);
        if (len == -1)
            return null;
        return DataUtils.readString(buff, len);
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        if (obj == null) {
            buff.putVarInt(-1);
            return;
        }
        String s = obj.toString();
        int len = s.length();
        buff.putVarInt(len).putStringData(s, len);
    }

}
