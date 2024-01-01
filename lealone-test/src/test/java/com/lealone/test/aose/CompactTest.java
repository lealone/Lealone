/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import org.junit.Test;

public class CompactTest extends AoseTestBase {
    @Test
    public void run() {
        init();

        // map.clear();
        //
        // map.put(1, "v1");
        // map.put(50, "v50");
        // map.put(100, "v100");

        // map.save();

        for (int i = 1; i <= 200; i++)
            map.put(i, "value" + i);
        map.save();
        // map.printPage();

        for (int i = 1; i <= 50; i++)
            map.put(i, "value" + i);

        map.save();
        // map.printPage();

        for (int i = 100; i <= 150; i++)
            map.put(i, "value" + i);

        map.save();
        // map.printPage();

        for (int i = 50; i <= 150; i++)
            map.put(i, "value" + i);

        map.save();
        // map.printPage();

        assertEquals(map.cursor(), 200);

        assertEquals(200, map.size());
    }
}
