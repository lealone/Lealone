/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.json;

import com.lealone.common.util.CamelCaseHelper;

public interface JsonFormat {

    public static final JsonFormat DEFAULT_FORMAT = new DefaultJsonFormat();

    public static final JsonFormat FRONTEND_FORMAT = new FrontendJsonFormat();

    public static final JsonFormat LOWER_UNDERSCORE_FORMAT = new LowerUnderscoreJsonFormat();

    public default Object encodeBoolean(Boolean v) {
        return v ? 1 : 0;
    }

    public default Boolean decodeBoolean(Object v) {
        return ((Number) v).byteValue() != 0;
    }

    public default boolean includesInternalFields() {
        return true;
    }

    public String convertName(String name);

    public static class DefaultJsonFormat implements JsonFormat {
        @Override
        public String convertName(String name) {
            return name; // 默认是UPPER_UNDERSCORE
        }
    }

    public static class LowerUnderscoreJsonFormat implements JsonFormat {
        @Override
        public String convertName(String name) {
            return name.toLowerCase();
        }
    }

    public static class FrontendJsonFormat implements JsonFormat {

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
}
