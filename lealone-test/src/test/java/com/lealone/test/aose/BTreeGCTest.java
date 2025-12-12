/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import com.lealone.db.index.standard.PrimaryKeyType;
import com.lealone.db.row.Row;
import com.lealone.db.row.RowType;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.type.StorageDataTypeFactory;

public class BTreeGCTest extends AoseTestBase {

    public static void main(String[] args) {
        new BTreeGCTest().run();
    }

    int count = 100 * 10000;

    // @Test
    public void run() {
        // putData();
        // testGc();
        // testSave();
        // testSplit();
        // testRead();
        // testRowType();
        // testMemory();
        // testConcurrent();
        testFullGc();
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
            map.fullGc();
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
        map.fullGc();
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

    public void testRowType() {
        int columnCount = 1;
        PrimaryKeyType kType = new PrimaryKeyType();
        RowType rType = new RowType(null, columnCount);
        rType.setRowOnly(true);
        ArrayList<Long> keys = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            Long key = Long.valueOf(i);
            keys.add(key);
        }
        // Collections.shuffle(keys);

        int cacheSize = 16; // 单位是MB
        pageSize = 2 * 1024; // 16K
        storage = openStorage(pageSize, cacheSize);
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(StorageSetting.IN_MEMORY.name(), "true");
        BTreeMap<Row, Row> map = storage.openBTreeMap("BTreeGCTest", kType, rType, parameters);
        // ConcurrentSkipListMap<Row, Row> map = new ConcurrentSkipListMap<>();
        long total = 0;
        int loop = 20;
        for (int n = 1; n <= loop * 2; n++) {
            map.clear();
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                long key = keys.get(i);

                String value = "value-" + key;

                Value[] columns = new Value[columnCount];
                columns[0] = ValueString.get(value);
                Row r = new Row(key, columns);
                map.put(r, r);
            }
            long t2 = System.currentTimeMillis();
            if (n > loop)
                total += (t2 - t1);
            System.out.println("put count: " + map.size() + ", time: " + (t2 - t1) + " ms");
        }
        System.out.println("total time: " + total + " ms, avg time: " + (total / loop) + " ms");
        // map.save();

        // for (int n = 1; n <= 50; n++) {
        // long t1 = System.currentTimeMillis();
        // for (int i = 0; i < count; i++) {
        // long key = keys.get(i);
        // Row r = new Row(key, null);
        // map.get(r);
        // }
        // long t2 = System.currentTimeMillis();
        // System.out.println("put count: " + map.size() + ", time: " + (t2 - t1) + " ms");
        // }
    }

    public void testSplit() {
        ArrayList<Integer> keys = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            keys.add(key);
        }
        Collections.shuffle(keys);

        openMap(true);
        // ConcurrentSkipListMap<Integer, String> map = new ConcurrentSkipListMap<>();
        for (int n = 1; n <= 50; n++) {
            map.clear();
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                Integer key = keys.get(i);
                String value = "value-" + i;
                map.put(key, value);
            }
            long t2 = System.currentTimeMillis();
            System.out.println("put count: " + map.size() + ", time: " + (t2 - t1) + " ms");
        }
        map.size();
        // map.save();
    }

    public void testRead() {
        ArrayList<Long> keys = new ArrayList<>(count);
        for (int i = 1; i <= count; i++) {
            Long key = Long.valueOf(i);
            keys.add(key);
        }

        openMap(true);
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(StorageSetting.IN_MEMORY.name(), "true");
        BTreeMap<Long, String> map = storage.openBTreeMap("testRead",
                StorageDataTypeFactory.getLongType(), null, parameters);
        // ConcurrentSkipListMap<Long, String> map = new ConcurrentSkipListMap<>();
        map.clear();
        for (int i = 0; i < count; i++) {
            Long key = keys.get(i);
            String value = "value-" + i;
            map.put(key, value);
        }

        // Collections.shuffle(keys);
        for (int n = 1; n <= 50; n++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                Long key = keys.get(i);
                map.get(key);
            }
            long t2 = System.currentTimeMillis();
            System.out.println("put count: " + map.size() + ", time: " + (t2 - t1) + " ms");
        }
    }

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
        map = storage.openBTreeMap("BTreeGCTest", StorageDataTypeFactory.getIntType(), null, parameters);
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

    public void testFullGc() {
        openMap();
        for (int i = 1; i <= 10000; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.save();
        map.getBTreeStorage().getBTreeGC().setMaxMemory(1);

        StorageMapCursor<Integer, String> cursor = map.cursor();
        while (cursor.next())
            cursor.getKey();
        map.gc();

        for (int i = 1; i <= 10000; i++) {
            map.get(i);
        }
        new Thread(() -> {
            map.put(1, "v1");
        }).start();
        map.gc();

        map.fullGc();
    }
}
