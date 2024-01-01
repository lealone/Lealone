/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage;

import java.util.Objects;
import java.util.function.Consumer;

import com.lealone.storage.page.IPage;

public interface StorageMapCursor<K, V> {

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    K getKey();

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    V getValue();

    default IPage getPage() {
        return null;
    }

    boolean next();

    default void forEachRemaining(Consumer<? super K> action) {
        Objects.requireNonNull(action);
        while (next())
            action.accept(getKey());
    }
}
