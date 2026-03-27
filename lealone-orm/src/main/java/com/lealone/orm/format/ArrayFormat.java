/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.Arrays;
import java.util.List;

public class ArrayFormat implements TypeFormat<Object[]> {

    @Override
    public Object encode(Object[] v) {
        return Arrays.asList(v);
    }

    @Override
    public Object[] decode(Object v) {
        if (v instanceof List)
            return ((List<?>) v).toArray();
        else if (v instanceof Object[])
            return (Object[]) v;
        else
            return new Object[] { v };
    }
}
