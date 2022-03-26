/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.test.TestBase;

public class CompactTest extends TestBase {

    private AOStorage storage;
    private BTreeMap<Integer, String> map;

    @Test
    public void run() {
        init();

        map = storage.openBTreeMap("CompactTest");

        // map.clear();
        //
        // map.put(1, "v1");
        // map.put(50, "v50");
        // map.put(100, "v100");

        // map.save();

        for (int i = 1; i <= 200; i++)
            map.put(i, "value" + i);
        map.save();
        map.printPage();

        for (int i = 1; i <= 50; i++)
            map.put(i, "value" + i);

        map.save();
        map.printPage();

        for (int i = 100; i <= 150; i++)
            map.put(i, "value" + i);

        map.save();
        map.printPage();

        for (int i = 50; i <= 150; i++)
            map.put(i, "value" + i);

        map.save();
        map.printPage();

        AtomicInteger count = new AtomicInteger();
        map.cursor().forEachRemaining(e -> {
            count.incrementAndGet();
        });
        assertEquals(200, count.get());

        assertEquals(200, map.size());
    }

    private void init() {
        int pageSplitSize = 16 * 1024;
        pageSplitSize = 4 * 1024;
        pageSplitSize = 1 * 1024;
        // pageSplitSize = 32 * 1024;
        storage = AOStorageTest.openStorage(pageSplitSize);
    }
}
