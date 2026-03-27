/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.sql.Timestamp;

public class TimestampFormat implements TypeFormat<Timestamp> {

    @Override
    public Object encode(Timestamp v) {
        return v.getTime();
    }

    @Override
    public Timestamp decode(Object v) {
        return new Timestamp(((Number) v).longValue());
    }
}
