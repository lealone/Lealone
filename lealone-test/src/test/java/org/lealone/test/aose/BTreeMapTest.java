/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.aose;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreePage;
import org.lealone.storage.aose.btree.PageReference;
import org.lealone.test.TestBase;

public class BTreeMapTest extends TestBase {
    private AOStorage storage;
    private String storageName;
    private BTreeMap<Integer, String> map;

    @Test
    public void run() {
        init();
        testMapOperations();
        testGetEndpointToKeyMap();
        testCompact();
        testTransfer();
        testSplit();
        testRemotePage();
        testLeafPageRemove();
    }

    private void init() {
        AOStorageBuilder builder = new AOStorageBuilder();
        storageName = joinDirs("aose");
        int pageSplitSize = 16 * 1024;
        pageSplitSize = 4 * 1024;
        pageSplitSize = 1 * 1024;
        // pageSplitSize = 32 * 1024;
        builder.storageName(storageName).compress().reuseSpace().pageSplitSize(pageSplitSize).minFillRate(30);
        storage = builder.openStorage();
        openMap();
    }

    private void openMap() {
        if (map == null || map.isClosed())
            map = storage.openBTreeMap("BTreeMapTest");
    }

    private BTreeMap<Integer, String> openMap(String name, Map<String, String> parameters) {
        return storage.openBTreeMap(name, null, null, parameters);
    }

    void testSplit() {
        openMap();
        map.clear();
        for (int i = 1; i <= 40; i += 2) {
            map.put(i, "value" + i);
        }

        map.save();

        for (int i = 1; i <= 40; i += 2) {
            map.remove(i);
        }

        map.printPage();
    }

    void testGetEndpointToKeyMap() {
        openMap();
        map.clear();
        testGetEndpointToKeyMap(map); // 测试空map

        map.clear();
        int count = 10;
        for (int i = 1; i <= count; i++) {
            map.put(i, "value" + i);
        }
        testGetEndpointToKeyMap(map); // 测试只有一个root leaf page的map

        map.clear();
        count = 6000;
        for (int i = 1; i <= count; i++) {
            map.put(i, "value" + i);
        }
        testGetEndpointToKeyMap(map); // 测试有root node page的map

        map.clear();

        ArrayList<PageKey> pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 1, 9, pageKeys);
        assertEquals(1, pageKeys.size());
        assertNull(pageKeys.get(0).key);

        map.put(1, "value" + 1);

        pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 1, 9, pageKeys);
        assertEquals(1, pageKeys.size());
        assertTrue(pageKeys.get(0).key.equals(1));

        for (int i = 1; i <= 40; i += 2) {
            map.put(i, "value" + i);
        }

        pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 1, 9, pageKeys);
        assertEquals(1, pageKeys.size());

        pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 15, 40, pageKeys);
        assertEquals(1, pageKeys.size());

        pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 15, null, pageKeys);
        assertEquals(1, pageKeys.size());

        // map.close();
    }

    void testGetEndpointToKeyMap(BTreeMap<Integer, String> map) {
        BTreePage root = map.getRootPage();
        Random random = new Random();
        String[] ids = { "a", "b", "c", "d", "e", "f" };
        injectReplicationHostIds(root, random, ids);
        Integer from = 3; // 5900;
        Integer to = 5999;
        HashSet<PageKey> pageKeySet = new HashSet<>();
        Map<String, List<PageKey>> endpointToPageKeyMap = map.getEndpointToPageKeyMap(null, from, to);
        // System.out.println(endpointToPageKeyMap);
        for (List<PageKey> pageKeys : endpointToPageKeyMap.values()) {
            for (PageKey pk : pageKeys) {
                if (!pageKeySet.add(pk)) {
                    // System.out.println("PageKey: " + pk);
                    fail("PageKey: " + pk);
                }
            }
        }

        int count = 0;
        for (List<PageKey> pageKeys : endpointToPageKeyMap.values()) {
            // System.out.println("pageKeys: " + pageKeys);
            StorageMapCursor<Integer, String> cursor = map.cursor(pageKeys, from);
            while (cursor.hasNext()) {
                count++;
                cursor.next();
                // System.out.println(cursor.getKey());
            }
        }
        System.out.println("count: " + count + ", to-from: " + (to - from + 1));
    }

    void injectReplicationHostIds(BTreePage page, Random random, String[] ids) {
        if (page.isLeaf()) {
            injectReplicationHostIds(null, page, random, ids);
            return;
        }
        for (PageReference pf : page.getChildren()) {
            if (pf.getPage().isNode())
                injectReplicationHostIds(pf.getPage(), random, ids);
            else {
                injectReplicationHostIds(pf, page, random, ids);
            }
        }
    }

    void injectReplicationHostIds(PageReference pf, BTreePage page, Random random, String[] ids) {
        int needNodes = 3;
        ArrayList<String> replicationHostIds = new ArrayList<>(needNodes);
        int totalNodes = ids.length;
        Set<Integer> indexSet = new HashSet<>(needNodes);
        while (true) {
            int i = random.nextInt(totalNodes);
            indexSet.add(i);
            if (indexSet.size() == needNodes)
                break;
        }
        for (int i : indexSet) {
            replicationHostIds.add(ids[i]);
        }
        page.setReplicationHostIds(replicationHostIds);
        if (pf != null)
            pf.setReplicationHostIds(replicationHostIds);
    }

    void testMapOperations() {
        Object v = null;
        map.clear();

        v = map.put(10, "a");
        assertNull(v);
        v = map.get(10);
        assertEquals("a", v);
        assertTrue(map.containsKey(10));

        v = map.putIfAbsent(10, "a1");
        assertNotNull(v);
        assertEquals("a", v);

        v = map.putIfAbsent(20, "b");
        assertNull(v);

        v = map.get(20);
        assertEquals("b", v);

        map.clear();
        assertEquals(0, map.size());

        for (int i = 1; i <= 200; i++) {
            map.put(i, "value" + i);
        }

        assertEquals(200, map.size());

        v = map.firstKey();
        assertEquals(1, v);
        v = map.lastKey();
        assertEquals(200, v);

        v = map.higherKey(101); // >"101"的最小key
        assertEquals(102, v);
        v = map.ceilingKey(101); // >="101"的最小key
        assertEquals(101, v);

        v = map.lowerKey(101); // <"101"的最大key
        assertEquals(100, v);
        v = map.floorKey(101); // <="101"的最大key
        assertEquals(101, v);

        v = map.replace(100, "value100a", "value100");
        assertFalse((boolean) v);
        v = map.replace(100, "value100", "value100a");
        assertTrue((boolean) v);
        v = map.get(100);
        assertEquals("value100a", v);
        v = map.replace(100, "value100a", "value100");
        assertTrue((boolean) v);

        StorageMapCursor<?, ?> cursor = map.cursor(null);
        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertEquals(200, count);

        cursor = map.cursor(151);
        count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        assertEquals(50, count);

        v = map.remove(150);
        assertNotNull(v);
        assertEquals(199, map.size());

        // map.printPage();
        // map.remove();

        map.close();

        assertTrue(map.isClosed());

        try {
            map.put(10, "a");
            fail();
        } catch (IllegalStateException e) {
            // e.printStackTrace();
        }
    }

    void testCompact() {
        map = storage.openBTreeMap("BTreeMapTest");

        map.clear();

        map.put(1, "v1");
        map.put(50, "v50");
        map.put(100, "v100");

        map.save();

        for (int i = 1; i <= 200; i++)
            map.put(i, "value" + i);
        map.save();

        // map.printPage();

        for (int i = 50; i <= 200; i++)
            map.put(i, "value" + i);

        map.save();
    }

    void testTransfer() {
        String file = storageName + File.separator + map.getName() + "TransferTo" + AOStorage.SUFFIX_AO_FILE;
        deleteFileRecursive(file);
        testTransferTo(file);
        testTransferFrom(file);
    }

    void testTransferTo(String file) {
        BTreeMap<Integer, String> map = storage.openBTreeMap("transfer_test_1");
        map.clear();
        for (int i = 1000; i < 4000; i++) {
            map.put(i, "value" + i);
        }
        map.save();

        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();
            int firstKey = 2000;
            int lastKey = 3000;
            raf.seek(raf.length());
            map.transferTo(channel, firstKey, lastKey);
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void testTransferFrom(String file) {
        BTreeMap<Integer, String> map = storage.openBTreeMap("transfer_test_2");
        map.clear();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel channel = raf.getChannel();

            map.transferFrom(channel, 0, raf.length());
            raf.close();

            // 会传输完整的一个Page，所以如果firstKey是在Page的中间，那么就会多出来前面的记录
            assertTrue(map.size() >= ((3000 - 2000 + 1)));
            assertTrue(map.containsKey(2000));
            assertTrue(map.containsKey(3000));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void testRemotePage() {
        String mapName = "RemotePageTest";
        String dir = storageName + File.separator + mapName;
        deleteFileRecursive(dir);

        Map<String, String> parameters = new HashMap<>();
        parameters.put("isShardingMode", "true");
        try {
            openMap(mapName, parameters);
            fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        parameters.put("initReplicationEndpoints", "127.0.0.1:1111&127.0.0.1:222");
        BTreeMap<Integer, String> map = openMap(mapName, parameters);
        assertTrue(map.getRootPage().isRemote());
        assertEquals(2, map.getRootPage().getReplicationHostIds().size());

        try {
            map.put(1, "abc");
            fail();
        } catch (Exception e) {
            System.out.println("RemotePage put: " + e.getMessage());
        }

        try {
            map.get(1);
            fail();
        } catch (Exception e) {
            System.out.println("RemotePage get: " + e.getMessage());
        }
    }

    void testLeafPageRemove() {
        openMap();
        map.clear();

        for (int i = 1; i <= 40; i += 2) {
            map.put(i, "value" + i);
        }

        for (int i = 1; i <= 40; i += 2) {
            map.remove(i);
        }

        for (int i = 1; i <= 40; i += 2) {
            map.put(i, "value" + i);
        }
        // 上面put的数据得到一个node page加两个leaf page
        ArrayList<PageKey> pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 1, 50, pageKeys);
        assertEquals(2, pageKeys.size());

        PageKey pk = pageKeys.get(0);
        map.removeLeafPage(pk);

        pk = pageKeys.get(1);
        map.removeLeafPage(pk);

        assertTrue(map.getRootPage().isEmpty());

        map.clear();

        // 测试多层node page
        for (int i = 1; i <= 500; i += 2) {
            map.put(i, "value" + i);
        }
        pageKeys = new ArrayList<>();
        map.getEndpointToPageKeyMap(null, 1, 500, pageKeys);
        for (PageKey pageKey : pageKeys)
            map.removeLeafPage(pageKey);
        assertTrue(map.getRootPage().isEmpty());
    }
}
