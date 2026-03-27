/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class LowerUnderscoreJsonFormat extends DefaultJsonFormat {

    public static final NameCaseFormat LOWER_UNDERSCORE = new NameCaseFormat() {
        @Override
        public String convert(String name) {
            return name.toLowerCase();
        }
    };

    @Override
    public NameCaseFormat getNameCaseFormat() {
        return LOWER_UNDERSCORE;
    }
}
