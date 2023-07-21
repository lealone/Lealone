/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage;

import java.util.Iterator;

import org.lealone.storage.page.IPage;

public interface StorageMapCursor<K, V> extends Iterator<K> {

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

    IPage getPage();

}
