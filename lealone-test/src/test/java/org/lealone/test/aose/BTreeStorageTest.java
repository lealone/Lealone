/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;

public class BTreeStorageTest extends AoseTestBase {
    @Test
    public void run() {
        init();
        testChunk();
    }

    private void put(int start, int end) {
        for (int i = start; i <= end; i++) {
            Integer key = i;
            String value = "value-" + i;
            map.put(key, value);
        }
        map.save();
    }

    private void testChunk() {
        int count = 500;
        put(1, 100);
        put(101, 200);
        put(201, count);
        put(count / 2, count);

        for (int i = 1; i <= count; i++) {
            assertNotNull(map.get(i));
        }

        map.remove();
    }

    @Test
    public void testClear() {
        init();

        openMap();
        map.clear();

        map.put(10, "a");
        map.close();

        openMap();
        map.clear();
        map.close();

        openMap();
        assertNull(map.get(10));
    }
}
