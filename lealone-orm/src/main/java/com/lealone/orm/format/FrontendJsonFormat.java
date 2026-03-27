/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import com.lealone.common.util.CamelCaseHelper;

public class FrontendJsonFormat extends DefaultJsonFormat {

    public static final BooleanFormat BOOLEAN_FORMAT = new BooleanFormat() {
        @Override
        public Object encode(Boolean value) {
            return value;
        }

        @Override
        public Boolean decode(Object v) {
            return (Boolean) v;
        }
    };

    public static final NameCaseFormat CAMEL = new NameCaseFormat() {
        @Override
        public String convert(String name) {
            return CamelCaseHelper.toCamelFromUnderscore(name);
        }
    };

    @Override
    public boolean includesInternalFields() {
        return false;
    }

    @Override
    public NameCaseFormat getNameCaseFormat() {
        return CAMEL;
    }

    @Override
    public BooleanFormat getBooleanFormat() {
        return BOOLEAN_FORMAT;
    }
}
