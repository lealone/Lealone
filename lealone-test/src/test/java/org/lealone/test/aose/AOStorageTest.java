/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.lealone.test.aose;

import java.util.HashMap;

import org.lealone.aose.storage.AOStorage;
import org.lealone.aose.storage.AOStorageBuilder;
import org.lealone.aose.storage.BufferedMap;
import org.lealone.aose.storage.btree.BTreeMap;
import org.lealone.aose.storage.rtree.RTreeMap;
import org.lealone.aose.storage.rtree.SpatialKey;
import org.lealone.common.util.DataUtils;
import org.lealone.db.value.ValueString;
import org.lealone.storage.StorageMapCursor;
import org.lealone.test.TestBase;

public class AOStorageTest extends TestBase {

    public static void main(String[] args) {
        new AOStorageTest().run();
    }

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }

    AOStorage storage;
    BTreeMap<String, String> map;
    RTreeMap<String> rmap;
    String storageName = joinDirs("aose");

    void run() {
        // testPagePos();
        openStorage();
        try {
            openMap();
            testPut();
            // testSplit();
            // testGet();

            // testPrintPage();

            // testBTreeMap();
            // testBufferedMap();

            // openRTreeMap();
            // testRTreePut();

            testBackupTo();
        } finally {
            storage.close();
        }
    }

    void testBackupTo() {
        String fileName = joinDirs("testBackup", "backup1.zip");
        storage.backupTo(fileName);
    }

    void testBufferedMap() {
        BufferedMap<Integer, String> bmap = storage.openBufferedMap("testBufferedMap", null, null, null);
        p(bmap.firstKey());

        for (int i = 10; i < 20; i += 2) {
            bmap.put(i, "value" + i);
        }

        bmap.merge();

        for (int i = 11; i < 20; i += 2) {
            bmap.put(i, "value" + i);
        }
        bmap.put(22, "value" + 22);

        int count = 0;

        StorageMapCursor<Integer, String> cursor = bmap.cursor(null);
        while (cursor.hasNext()) {
            cursor.next();
            p(cursor.getKey());
            p(cursor.getValue());
            count++;
        }

        assertEquals(5 + 5 + 1, count);
        bmap.remove();
    }

    void testBTreeMap() {
        Object v = null;
        map.clear();

        v = map.put("10", "a");
        p(v);

        v = map.putIfAbsent("10", "a1");
        p(v);

        v = map.putIfAbsent("20", "b");
        p(v);

        v = map.get("20");
        p(v);

        map.clear();

        for (int i = 100; i < 300; i++) {
            map.put("" + i, "value" + i);
        }

        v = map.get("105");
        p(v);

        v = map.firstKey();
        p(v);
        v = map.lastKey();
        p(v);

        v = map.higherKey("101"); // >"101"的最小key
        p(v);
        v = map.ceilingKey("101"); // >="101"的最小key
        p(v);

        v = map.lowerKey("101"); // <"101"的最大key
        p(v);
        v = map.floorKey("101"); // <="101"的最大key
        p(v);

        v = map.replace("100", "value100a", "value100");
        p(v);
        v = map.replace("100", "value100", "value100a");
        p(v);
        v = map.replace("100", "value100a", "value100");
        p(v);

        map.clear();
    }

    void testPagePos() {
        int chunkId = 2;
        int offset = 10;
        int length = 30;
        int type = 1;

        long pos = DataUtils.getPagePos(chunkId, offset, length, type);
        int pageMaxLength = DataUtils.getPageMaxLength(pos);
        assertEquals(chunkId, DataUtils.getPageChunkId(pos));
        assertEquals(offset, DataUtils.getPageOffset(pos));
        assertEquals(32, pageMaxLength);
        assertEquals(type, DataUtils.getPageType(pos));
    }

    void openStorage() {
        AOStorageBuilder builder = new AOStorageBuilder();
        builder.storageName(storageName);
        builder.compress();
        builder.pageSplitSize(1024);
        builder.encryptionKey("mykey".toCharArray());
        // builder.inMemory();
        storage = builder.openStorage();
    }

    void openMap() {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put("isShardingMode", "true");
        parameters.put("initReplicationEndpoints", "a&b&c");
        map = storage.openBTreeMap("testBTreeMap", ValueString.type, ValueString.type, parameters);
        p(storage.getMapNames());
    }

    void openRTreeMap() {
        rmap = storage.openRTreeMap("testRTreeMap", ValueString.type, 3);
        p(storage.getMapNames());
    }

    void testRTreePut() {
        for (int i = 10; i < 100; i++) {
            rmap.put(new SpatialKey(i, i * 1.0F, i * 2.0F), "value" + i); // TODO 还有bug，h2也有
        }
        rmap.save();

        p(rmap.size());
        p(new SpatialKey(1, 1 * 1.0F, 1 * 2.0F));
    }

    void testPrintPage() {
        for (int i = 10; i < 200; i++) {
            map.put("" + i, "value" + i);
        }
        map.save();

        map.printPage();
    }

    void testSplit() {
        map.clear();

        map.put("10", "value10");
        map.put("50", "value50");

        for (int i = 51; i < 100; i += 2) {
            map.put("" + i, "value" + i);
        }
        map.save();

        for (int i = 11; i < 50; i += 2) {
            map.put("" + i, "value" + i);
        }
        map.save();

        // for (int i = 10; i < 100; i += 2) {
        // map.put("" + i, "value" + i);
        // }
        //
        // map.save();

        // for (int i = 11; i < 100; i += 2) {
        // map.put("" + i, "value" + i);
        // }
        //
        // map.save();
    }

    void testPut() {
        for (int i = 10; i < 1000; i++) {
            map.put("" + i, "value" + i);
        }

        map.save();

        // for (int i = 100; i < 200; i++) {
        // map.put("" + i, "value" + i);
        // }

        map.put("" + 10, "value" + 10);

        map.put("" + 30, "value" + 30);

        p(map.size());
    }

    void testGet() {
        p(map.get("50"));

        String key = map.firstKey();
        p(key);

        key = map.lastKey();
        p(key);

        key = map.higherKey("30");
        p(key);
        key = map.ceilingKey("30");
        p(key);

        key = map.floorKey("30");
        p(key);
        key = map.lowerKey("30");
        p(key);

        // for (String k : map.keyList())
        // p(k);
    }
}
