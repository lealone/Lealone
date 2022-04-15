/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.test.TestBase;

public abstract class AoseTestBase extends TestBase {

    protected AOStorage storage;
    protected BTreeMap<Integer, String> map;
    protected int pageSplitSize = 1 * 1024;

    protected void init() {
        init(false);
    }

    protected void init(boolean clearMap) {
        init(getClass().getSimpleName(), clearMap);
    }

    protected void init(String mapName) {
        init(mapName, false);
    }

    protected void init(String mapName, boolean clearMap) {
        storage = AOStorageTest.openStorage(pageSplitSize);
        map = storage.openBTreeMap(mapName);
        if (clearMap)
            map.clear();
    }

    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = storage.openBTreeMap(getClass().getSimpleName());
        }
    }

    protected void assertEquals(StorageMapCursor<?, ?> cursor, int expectedSsize) {
        AtomicInteger count = new AtomicInteger();
        cursor.forEachRemaining(e -> {
            count.incrementAndGet();
        });
        assertEquals(expectedSsize, count.get());
    }
}
