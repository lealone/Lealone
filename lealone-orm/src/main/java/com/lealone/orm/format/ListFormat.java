/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.List;

public class ListFormat<E> implements TypeFormat<List<E>> {

    @Override
    public Object encode(List<E> v) {
        return v;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<E> decode(Object v) {
        return (List<E>) v;
    }
}
