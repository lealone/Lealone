/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

public class BTreeGCTest extends AoseTestBase {

    public static void main(String[] args) {
        new BTreeGCTest().run();
    }

    // @Test
    public void run() {
        putData();
        // testGc();
        testSave();
    }

    public void testGc() {
        Integer key = 10;
        map.get(key);
        key = 11;
        map.get(key);
        map.fullGc(false);
        map.get(key);
    }

    public void testSave() {
        putData();
        long dirtyMemory = map.collectDirtyMemory();
        System.out.println("dirtyMemory: " + dirtyMemory);

        Integer key = 10;
        String value = "value" + key * 100;
        map.put(key, value);
        key = 100;
        value = "value" + key * 100;
        map.put(key, value);

        dirtyMemory = map.collectDirtyMemory();
        System.out.println("dirtyMemory: " + dirtyMemory);
        map.save(dirtyMemory);
    }

    int count = 100 * 10000;

    public void putData() {
        int cacheSize = 16; // 单位是MB
        pageSize = 16 * 1024; // 16K
        storage = openStorage(pageSize, cacheSize);
        map = storage.openBTreeMap("BTreeGCTest");
        if (!map.isEmpty())
            return;
        long t1 = System.currentTimeMillis();
        long total = t1;
        int saveCount = 0;
        for (int i = 1; i <= count; i++) {
            Integer key = i;// random.nextInt(count);
            String value = "value-" + i;
            map.put(key, value);
            if (i % (100 * 10000) == 0 || i == count) {
                long t2 = System.currentTimeMillis();
                saveCount++;
                System.out.println("save count: " + saveCount + " time: " + (t2 - t1) + " ms");
                t1 = t2;
                // map.save();
            }
        }
        System.out.println(
                "put count: " + count + " total time: " + (System.currentTimeMillis() - total) + " ms");
        map.save();
    }
}
