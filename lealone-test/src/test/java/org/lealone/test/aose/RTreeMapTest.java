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
        RTreeMap<String> rmap = storage.openRTreeMap(name, ValueString.type, 3);
        assertEquals(rmap, storage.getMap(name));
        assertTrue(storage.hasMap(name));

        for (int i = 1; i <= 100; i++) {
            SpatialKey key = new SpatialKey(i, i * 1.0F, i * 2.0F);
            // p(key);
            rmap.put(key, "value" + i);
        }
        // rmap.save(); // TODO 还有bug，h2也有

        assertEquals(100, rmap.size());
    }
}
