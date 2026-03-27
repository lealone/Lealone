/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class ByteFormat implements TypeFormat<Byte> {

    @Override
    public Object encode(Byte v) {
        return v;
    }

    @Override
    public Byte decode(Object v) {
        return ((Number) v).byteValue();
    }
}
