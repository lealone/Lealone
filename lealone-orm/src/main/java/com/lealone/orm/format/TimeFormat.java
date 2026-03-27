/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.sql.Time;

public class TimeFormat implements TypeFormat<Time> {

    @Override
    public Object encode(Time v) {
        return v.getTime();
    }

    @Override
    public Time decode(Object v) {
        return new Time(((Number) v).longValue());
    }
}
