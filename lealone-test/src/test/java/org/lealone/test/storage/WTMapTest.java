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

import org.lealone.storage.WTMap;
import org.lealone.test.UnitTestBase;

import com.wiredtiger.db.Connection;
import com.wiredtiger.db.Session;

public class WTMapTest extends UnitTestBase {
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
            assertNull(old);

            assertEquals("a", map.get(1));

            old = map.putIfAbsent(1, "b");
            assertEquals("a", old);

            old = map.putIfAbsent(2, "b");
            assertNull(old);
            assertEquals("b", map.get(2));

            old = map.remove(2);
            assertEquals("b", old);
            old = map.remove(2);
            assertNull(old);

            assertFalse(map.replace(1, "aa", "a1"));
            assertTrue(map.replace(1, "a", "a1"));
            assertEquals("a1", map.get(1));

            assertFalse(map.containsKey(2));
            assertTrue(map.containsKey(1));

            assertFalse(map.isEmpty());
            map.remove(1);
            assertTrue(map.isEmpty());

            assertEquals(0, map.size());
            map.put(1, "a");
            map.put(2, "b");
            map.put(3, "c");
            assertEquals(3, map.size());

            assertEquals(1, (int) map.firstKey());
            assertEquals(3, (int) map.lastKey());

            assertEquals(2, (int) map.lowerKey(3));
            assertEquals(3, (int) map.floorKey(3)); //<=3
            assertEquals(3, (int) map.higherKey(2));
            assertEquals(2, (int) map.ceilingKey(2)); //>=2

            //索引从0开始
            assertEquals(1, map.getKeyIndex(2));
            Integer key = map.getKey(2); //索引为2的key是3
            assertEquals(3, (int) key);
            key = map.getKey(-100);
            assertNull(key);
            key = map.getKey(4);
            assertNull(key);

            assertTrue(map.areValuesEqual("a", "a"));
            assertFalse(map.areValuesEqual("a", "b"));

            org.lealone.storage.StorageMap.Cursor<Integer, String> cursor = map.cursor(2);
            assertTrue(cursor.hasNext());
            key = cursor.next();
            assertEquals(2, (int) key);
            assertEquals(2, (int) cursor.getKey());
            assertEquals("b", cursor.getValue());
            assertTrue(cursor.hasNext());
            cursor = map.cursor(null);
            assertTrue(cursor.hasNext());

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
