/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import java.util.ArrayList;

import org.junit.Test;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.page.PageKey;

public class PageKeyCursorTest extends AoseTestBase {
    @Test
    public void run() {
        init();

        int size = 60;
        for (int i = 1; i <= size; i++) {
            String v = "value" + i;
            map.put(i, v);
        }
        // map.printPage();

        ArrayList<PageKey> pageKeys = new ArrayList<>();
        PageKey pk1 = new PageKey(8, true);
        PageKey pk2 = new PageKey(22, true);
        PageKey pk3 = new PageKey(50, false);
        pageKeys.add(pk1);
        pageKeys.add(pk2);
        pageKeys.add(pk3);

        CursorParameters<Integer> parameters = CursorParameters.create(null, pageKeys);
        assertEquals(map.cursor(parameters), 25);
    }
}
