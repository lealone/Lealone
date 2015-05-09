/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.lealone.test.storage;

import java.util.Map.Entry;

import org.junit.Assert;
import org.lealone.engine.WTMap;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.Session;

public class WTMapTest {
    public static void main(String[] args) {
        new WTMapTest().run();
    }

    //@Test
    public void run() {
        Connection wtConnection = WiredTigerTest.getWTConnection();
        Session wtSession = wtConnection.open_session(null);

        try {

            WTMap<Integer, String> map = new WTMap<>(wtSession, "WTMapTest");
            map.clear();

            System.out.println("name = " + map.getName() + ", id = " + map.getId());

            String old = map.put(1, "a");
            Assert.assertNull(old);

            Assert.assertEquals("a", map.get(1));

            old = map.putIfAbsent(1, "b");
            Assert.assertEquals("a", old);

            old = map.putIfAbsent(2, "b");
            Assert.assertNull(old);
            Assert.assertEquals("b", map.get(2));

            old = map.remove(2);
            Assert.assertEquals("b", old);
            old = map.remove(2);
            Assert.assertNull(old);

            Assert.assertFalse(map.replace(1, "aa", "a1"));
            Assert.assertTrue(map.replace(1, "a", "a1"));
            Assert.assertEquals("a1", map.get(1));

            Assert.assertFalse(map.containsKey(2));
            Assert.assertTrue(map.containsKey(1));

            Assert.assertFalse(map.isEmpty());
            map.remove(1);
            Assert.assertTrue(map.isEmpty());

            Assert.assertEquals(0, map.size());
            map.put(1, "a");
            map.put(2, "b");
            map.put(3, "c");
            Assert.assertEquals(3, map.size());

            Assert.assertEquals(1, (int) map.firstKey());
            Assert.assertEquals(3, (int) map.lastKey());

            Assert.assertEquals(2, (int) map.lowerKey(3));
            Assert.assertEquals(3, (int) map.floorKey(3)); //<=3
            Assert.assertEquals(3, (int) map.higherKey(2));
            Assert.assertEquals(2, (int) map.ceilingKey(2)); //>=2

            //索引从0开始
            Assert.assertEquals(1, map.getKeyIndex(2));
            Integer key = map.getKey(2); //索引为2的key是3
            Assert.assertEquals(3, (int) key);
            key = map.getKey(-100);
            Assert.assertNull(key);
            key = map.getKey(4);
            Assert.assertNull(key);

            Assert.assertTrue(map.areValuesEqual("a", "a"));
            Assert.assertFalse(map.areValuesEqual("a", "b"));

            org.lealone.engine.StorageMap.Cursor<Integer, String> cursor = map.cursor(2);
            Assert.assertTrue(cursor.hasNext());
            key = cursor.next();
            Assert.assertEquals(2, (int) key);
            Assert.assertEquals(2, (int) cursor.getKey());
            Assert.assertEquals("b", cursor.getValue());
            Assert.assertTrue(cursor.hasNext());
            cursor = map.cursor(null);
            Assert.assertTrue(cursor.hasNext());

            for (Entry<Integer, String> e : map.entrySet()) {
                System.out.println("key = " + e.getKey() + ", value = " + e.getValue());
            }

            //performance(map);

        } finally {
            wtSession.close(null);
            wtConnection.close(null);
        }
    }

    void performance(WTMap<Integer, String> map) {
        long t = System.currentTimeMillis();
        int count = 5000;
        for (int i = 1; i <= count; i++) {
            map.put(i, "V" + (i * 10));
        }
        System.out.println("put " + count + " keys, elapsed " + (System.currentTimeMillis() - t) + " ms");
    }
}
