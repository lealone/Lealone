/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.sql.Date;

public class DateFormat implements TypeFormat<Date> {

    @Override
    public Object encode(Date v) {
        return v.getTime();
    }

    @Override
    public Date decode(Object v) {
        return new Date(((Number) v).longValue());
    }
}
