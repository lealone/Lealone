/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SetFormat<E> implements TypeFormat<Set<E>> {

    @Override
    public Object encode(Set<E> v) {
        return v;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<E> decode(Object v) {
        if (v instanceof List)
            return new HashSet<>((List<E>) v);
        return (Set<E>) v;
    }
}
