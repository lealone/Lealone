/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aose;

import org.junit.Test;

import com.lealone.test.TestBase;
import com.lealone.test.misc.CRUDExample;

public class MemoryStorageTest extends AoseTestBase {
    @Test
    public void run() throws Exception {
        TestBase test = new TestBase();
        test.setInMemory(true);
        test.setEmbedded(true);
        test.printURL();
        CRUDExample.crud(test.getConnection());
    }
}
