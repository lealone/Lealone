/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class ShortFormat implements TypeFormat<Short> {

    @Override
    public Object encode(Short v) {
        return v;
    }

    @Override
    public Short decode(Object v) {
        return ((Number) v).shortValue();
    }
}
