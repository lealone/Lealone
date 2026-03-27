/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class StringFormat implements TypeFormat<String> {

    @Override
    public Object encode(String v) {
        return v;
    }

    @Override
    public String decode(Object v) {
        return v.toString();
    }

}
