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
        testChunkMetaData();
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
