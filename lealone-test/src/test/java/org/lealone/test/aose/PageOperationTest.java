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

public class PageOperationTest extends TestBase {

    private AOStorage storage;
    private BTreeMap<Integer, String> map;

    @Test
    public void run() {
        init();
        testAddChild();
    }

    private void init() {
        int pageSplitSize = 1 * 1024;
        storage = AOStorageTest.openStorage(pageSplitSize);
        map = storage.openBTreeMap("PageOperationTest");
        map.clear();
    }

    // fix https://github.com/lealone/Lealone/issues/142
    // 当 root page 是 node page，重新打开 map 时，leaf page 找不到 ParentRef
    private void testAddChild() {
        for (int i = 1; i <= 30; i++)
            map.put(i, "value" + i);
        map.save();
        map.close();
        map = storage.openBTreeMap("PageOperationTest");

        int size = 60;
        for (int i = 31; i <= size; i++)
            map.put(i, "value" + i);

        AtomicInteger count = new AtomicInteger();
        map.cursor().forEachRemaining(e -> {
            count.incrementAndGet();
        });
        assertEquals(size, count.get());

        assertEquals(size, map.size());
    }
}
