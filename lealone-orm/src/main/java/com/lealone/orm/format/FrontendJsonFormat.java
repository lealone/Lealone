/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import com.lealone.common.util.CamelCaseHelper;

public class FrontendJsonFormat implements JsonFormat {

    @Override
    public Object encodeBoolean(Boolean v) {
        return v;
    }

    @Override
    public Boolean decodeBoolean(Object v) {
        return (Boolean) v;
    }

    @Override
    public boolean includesInternalFields() {
        return false;
    }

    @Override
    public String convertName(String name) {
        return CamelCaseHelper.toCamelFromUnderscore(name);
    }
}
