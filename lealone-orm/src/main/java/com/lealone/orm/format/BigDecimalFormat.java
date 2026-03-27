/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.math.BigDecimal;

public class BigDecimalFormat implements TypeFormat<BigDecimal> {

    @Override
    public Object encode(BigDecimal v) {
        return v.toString();
    }

    @Override
    public BigDecimal decode(Object v) {
        return new BigDecimal(v.toString());
    }
}
