/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class FloatFormat implements TypeFormat<Float> {

    @Override
    public Object encode(Float v) {
        return v;
    }

    @Override
    public Float decode(Object v) {
        return ((Number) v).floatValue();
    }
}
