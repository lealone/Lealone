/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

public class BTreeGCTest extends AoseTestBase {
    // @Test
    public void run() {
        int cacheSize = 16; // 单位是MB
        pageSplitSize = 16 * 1024; // 16K
        storage = AOStorageTest.openStorage(pageSplitSize, cacheSize);
        map = storage.openBTreeMap("BTreeGCTest");
        map.clear();

        int count = 5000 * 10000;
        long t1 = System.currentTimeMillis();
        long total = t1;
        int saveCount = 0;
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
            if (i % (100 * 10000) == 0 || i == count) {
                long t2 = System.currentTimeMillis();
                saveCount++;
                System.out.println("save count: " + saveCount + " time: " + (t2 - t1) + " ms");
                t1 = t2;
                map.save();
            }
        }
        System.out.println(
                "put count: " + count + " total time: " + (System.currentTimeMillis() - total) + " ms");
    }
}
