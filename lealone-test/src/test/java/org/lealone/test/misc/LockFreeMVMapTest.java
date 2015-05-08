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
package org.lealone.test.misc;

import java.io.File;

import org.lealone.mvstore.LockFreeMVMap;
import org.lealone.mvstore.MVStore;
import org.lealone.type.StringDataType;

public class LockFreeMVMapTest {
    public static void main(String[] args) {
        new LockFreeMVMapTest().run();
    }

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }

    MVStore store;
    LockFreeMVMap<String, String> map;
    String fileName = "./lealone-test-data/mvstore/LockFreeMVMapTest.mv.db";

    void initMVStore() {
        new File(fileName).getParentFile().mkdirs();
        MVStore.Builder builder = new MVStore.Builder();
        builder.fileName(fileName);
        builder.compress();
        builder.autoCommitDisabled();
        builder.pageSplitSize(1024);
        store = builder.open();
    }

    void run() {
        initMVStore();
        try {
            openMap();

            testPut();
            testGet();
        } finally {
            LockFreeMVMap.stopMerger();
            store.close();
        }
    }

    void openMap() {
        LockFreeMVMap.Builder<String, String> builder = new LockFreeMVMap.Builder<String, String>();
        builder.keyType(StringDataType.INSTANCE);
        builder.valueType(StringDataType.INSTANCE);

        map = store.openMap("test", builder);
    }

    void testPut() {
        for (int i = 10; i < 100; i++) {
            map.put("" + i, "value" + i);
        }
        store.commit();

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

        //        for (String k : map.keyList())
        //            p(k);

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
