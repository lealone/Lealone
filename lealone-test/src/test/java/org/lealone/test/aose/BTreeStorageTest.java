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
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.test.TestBase;

public class BTreeStorageTest extends TestBase {

    private AOStorage storage;
    private BTreeMap<Integer, String> map;

    @Test
    public void run() {
        init();
        testChunkMetaData();
    }

    private void init() {
        int pageSplitSize = 1 * 1024;
        storage = AOStorageTest.openStorage(pageSplitSize);
        openMap();
    }

    private void openMap() {
        if (map == null || map.isClosed()) {
            map = storage.openBTreeMap("BTreeStorageTest");
        }
    }

    void testChunkMetaData() {
        int count = 500;
        for (int i = 1; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.save();

        for (int i = count / 2; i <= count; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.save();

        map.remove();
    }
}
