/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class LowerUnderscoreJsonFormat implements JsonFormat {

    @Override
    public String convertName(String name) {
        return name.toLowerCase();
    }
}
