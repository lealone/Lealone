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

import org.junit.Test;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.BufferedMap;
import org.lealone.test.TestBase;

public class BufferedMapTest extends TestBase {

    private AOStorage storage;
    private BufferedMap<Integer, String> bmap;

    @Test
    public void run() {
        init();
        testCursor();
        testAppend();
        bmap.remove();
    }

    private void init() {
        storage = AOStorageTest.openStorage();
        String name = "BufferedMapTest";
        bmap = storage.openBufferedMap(name, null, null, null);
        assertEquals(bmap, storage.getMap(name));
        assertTrue(storage.hasMap(name));
    }

    void testCursor() {
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
            // p(cursor.getKey());
            // p(cursor.getValue());
            count++;
        }

        assertEquals(5 + 5 + 1, count);
    }

    void testAppend() {
        String name = "BufferedMapTest_testAppend";
        BufferedMap<ValueLong, String> bmap = storage.openBufferedMap(name, null, null, null);
        assertEquals(0, bmap.getMaxKeyAsLong());

        bmap.put(ValueLong.get(11), "abc");
        assertEquals(11, bmap.getMaxKeyAsLong());

        bmap.append("cdf");
        assertEquals(12, bmap.getMaxKeyAsLong());

        bmap.put(ValueLong.get(100), "abc100");
        assertEquals(100, bmap.getMaxKeyAsLong());

        bmap.append("cdf");
        assertEquals(101, bmap.getMaxKeyAsLong());
    }
}
