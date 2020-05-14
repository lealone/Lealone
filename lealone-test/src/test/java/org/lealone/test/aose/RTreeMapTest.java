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
import org.lealone.db.value.ValueString;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.rtree.RTreeMap;
import org.lealone.storage.aose.rtree.SpatialKey;
import org.lealone.test.TestBase;

public class RTreeMapTest extends TestBase {
    @Test
    public void run() {
        AOStorage storage = AOStorageTest.openStorage();
        String name = "RTreeMapTest";
        int dimensions = 3;
        RTreeMap<String> rmap = storage.openRTreeMap(name, ValueString.type, dimensions);
        assertEquals(rmap, storage.getMap(name));
        assertTrue(storage.hasMap(name));
        rmap.clear();

        int count = 300;
        for (int i = 1; i <= count; i++) {
            // SpatialKey右边的参数必须是dimensions*2
            SpatialKey key = getSpatialKey(i);
            // p(key);
            String value = "value" + i;
            rmap.put(key, value);
        }

        assertEquals(300, rmap.size());

        int id = 10;
        String value = "value" + id;
        SpatialKey key = getSpatialKey(id);
        assertEquals(value, rmap.get(key));
        assertEquals(value, rmap.remove(key));
        assertEquals(count - 1, rmap.size());

        rmap.save();
    }

    private SpatialKey getSpatialKey(int i) {
        return new SpatialKey(i, i * 1.0F, i * 2.0F, i * 3.0F, i * 4.0F, i * 5.0F, i * 6.0F);
    }
}
