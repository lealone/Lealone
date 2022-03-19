/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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

    private void testChunkMetaData() {
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
