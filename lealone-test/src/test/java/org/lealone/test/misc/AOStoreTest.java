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
package org.lealone.test.misc;

import org.lealone.aostore.AOStore;
import org.lealone.aostore.btree.BTreeMap;
import org.lealone.aostore.rtree.RTreeMap;
import org.lealone.aostore.rtree.SpatialKey;
import org.lealone.storage.type.StringDataType;
import org.lealone.test.TestBase;

public class AOStoreTest {
    public static void main(String[] args) {
        new AOStoreTest().run();
    }

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }

    AOStore store;
    BTreeMap<String, String> map;
    RTreeMap<String> rmap;
    String storeName = TestBase.TEST_DIR + "/aostore";

    void run() {
        openStore();
        try {
            openMap();
            testPut();
            testGet();

            // openRTreeMap();
            // testRTreePut();
        } finally {
            store.close();
        }
    }

    void openStore() {
        AOStore.Builder builder = new AOStore.Builder();
        builder.storeName(storeName);
        builder.compress();
        builder.autoCommitDisabled();
        // builder.pageSplitSize(1024);
        store = builder.open();
    }

    void openMap() {
        BTreeMap.Builder<String, String> builder = new BTreeMap.Builder<String, String>();
        builder.keyType(StringDataType.INSTANCE);
        builder.valueType(StringDataType.INSTANCE);
        map = store.openMap("test", builder);

        p(store.getMapNames());
    }

    void openRTreeMap() {
        RTreeMap.Builder<String> rbuilder = new RTreeMap.Builder<String>();
        rbuilder.dimensions(3);
        rbuilder.valueType(StringDataType.INSTANCE);
        rmap = store.openMap("rtest", rbuilder);

        p(store.getMapNames());
    }

    void testRTreePut() {
        for (int i = 10; i < 100; i++) {
            rmap.put(new SpatialKey(i, i * 1.0F, i * 2.0F), "value" + i); // TODO 还有bug，h2也有
        }
        rmap.commit();

        p(rmap.size());
        p(new SpatialKey(1, 1 * 1.0F, 1 * 2.0F));
    }

    void testPut() {
        for (int i = 10; i < 100; i++) {
            map.put("" + i, "value" + i);
        }
        map.commit();

        for (int i = 100; i < 200; i++) {
            map.put("" + i, "value" + i);
        }

        map.remove("10");
        p(map.size());
    }

    void testGet() {
        String key = map.firstKey();
        p(key);

        key = map.lastKey();
        p(key);

        key = map.getKey(20);
        p(key);

        // for (String k : map.keyList())
        // p(k);

        long index = map.getKeyIndex("30");
        p(index);

        index = map.getKeyIndex("300");
        p(index);

        key = map.higherKey("30");
        p(key);
        key = map.ceilingKey("30");
        p(key);

        key = map.floorKey("30");
        p(key);
        key = map.lowerKey("30");
        p(key);
    }
}
