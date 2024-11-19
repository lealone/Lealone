/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import java.util.HashMap;

import com.lealone.storage.StorageSetting;

public class BTreeGCTest extends AoseTestBase {

    public static void main(String[] args) {
        new BTreeGCTest().run();
    }

    // @Test
    public void run() {
        // putData();
        // testGc();
        // testSave();
        // testSplit();
        // testMemory();
        testConcurrent();
    }

    public void testConcurrent() {
        openMap();
        map.clear();
        Integer key = 10;
        String value = "value" + key;
        map.put(key, value);
        map.save();
        map.get(key);
        Thread t1 = new Thread(() -> {
            map.fullGc(false);
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            map.put(key, value);
        });
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void testGc() {
        Integer key = 10;
        map.get(key);
        key = 11;
        map.get(key);
        map.fullGc(false);
        map.get(key);
    }

    public void testMemory() {
        openMap();
        map.clear();
        printUsedMemory();

        Integer key = 10;
        String value = "value" + key;
        map.put(key, value);
        value = "value" + key * 100;
        map.put(key, value);

        printUsedMemory();

        map.remove(key);

        printUsedMemory();

        int count = 1000;
        for (int i = 1; i <= count; i++) {
            key = i;
            value = "value-" + i;
            map.put(key, value);
        }
        printUsedMemory();

        for (int i = 1; i <= count; i++) {
            key = i;
            map.remove(key);
        }
        printUsedMemory();
    }

    private void printUsedMemory() {
        System.out.println("Used Memory: " + map.getMemorySpaceUsed());
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

    public void testSplit() {
        // Random random = new Random();
        openMap(true);
        // ConcurrentSkipListMap<Integer, String> map = new ConcurrentSkipListMap<>();
        for (int n = 1; n <= 50; n++) {
            map.clear();
            long t1 = System.currentTimeMillis();
            for (int i = 1; i <= count; i++) {
                Integer key = i;
                // key = random.nextInt(count);
                String value = "value-" + i;
                map.put(key, value);
            }
            long t2 = System.currentTimeMillis();
            System.out.println("put count: " + count + ", time: " + (t2 - t1) + " ms");
        }
        map.size();
        // map.save();
    }

    int count = 100 * 10000;

    @Override
    public void openMap() {
        openMap(false);
    }

    public void openMap(boolean inMemory) {
        int cacheSize = 16; // 单位是MB
        pageSize = 2 * 1024; // 16K
        storage = openStorage(pageSize, cacheSize);
        HashMap<String, String> parameters = new HashMap<>();
        if (inMemory)
            parameters.put(StorageSetting.IN_MEMORY.name(), "true");
        map = storage.openBTreeMap("BTreeGCTest", null, null, parameters);
    }

    public void putData() {
        openMap();
        // if (!map.isEmpty())
        // return;
        // ConcurrentSkipListMap<Integer, String> map = new ConcurrentSkipListMap<>();
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
        // map.save();
    }
}
