/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class DefaultJsonFormat implements JsonFormat {

    @Override
    public String convertName(String name) {
        return name; // 默认是UPPER_UNDERSCORE
    }
}
