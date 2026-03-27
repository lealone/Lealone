/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public interface TypeFormat<T> {

    public Object encode(T v);

    public T decode(Object v);

}
