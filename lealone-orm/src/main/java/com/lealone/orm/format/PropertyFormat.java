/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.Map;

public class PropertyFormat {

    /**
     * @return true 已经编码完成；false 没有编码，让后续的编码器继续处理
     */
    public boolean encode(Map<String, Object> map, String name, Object value) {
        return false;
    }
}
