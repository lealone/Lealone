/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.Map;

public class MapFormat<K, V> implements TypeFormat<Map<K, V>> {

    @Override
    public Object encode(Map<K, V> v) {
        return v;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<K, V> decode(Object v) {
        return (Map<K, V>) v;
    }
}
