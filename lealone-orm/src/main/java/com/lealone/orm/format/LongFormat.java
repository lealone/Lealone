/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class LongFormat implements TypeFormat<Long> {

    @Override
    public Object encode(Long v) {
        return v;
    }

    @Override
    public Long decode(Object v) {
        return ((Number) v).longValue();
    }
}
