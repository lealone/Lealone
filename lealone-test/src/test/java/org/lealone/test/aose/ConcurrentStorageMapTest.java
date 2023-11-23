/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.storage.StorageMap;

public class ConcurrentStorageMapTest extends AoseTestBase {

    protected final String mapName = getClass().getSimpleName();

    @Test
    public void run() throws Exception {
        storage = openStorage();
        Thread t1 = new Thread(() -> {
            testSyncOperations("a");
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            testSyncOperations("b");
        });
        t2.start();
        t1.join();
        t2.join();
    }

    void testSyncOperations(String v) {
        StorageMap<Integer, String> map = storage.openMap(mapName);
        map.put(10, v);
        v = map.get(10);
        assertTrue("a".equals(v) || "b".equals(v));
        assertTrue(map.containsKey(10));
        map.clear();
    }
}
