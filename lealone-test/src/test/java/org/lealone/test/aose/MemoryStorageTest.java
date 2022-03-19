/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.storage.memory.MemoryStorageEngine;
import org.lealone.test.TestBase;
import org.lealone.test.misc.CRUDExample;

public class MemoryStorageTest extends TestBase {

    @Test
    public void run() throws Exception {
        TestBase test = new TestBase();
        test.setStorageEngineName(MemoryStorageEngine.NAME);
        test.setEmbedded(true);
        test.printURL();
        CRUDExample.crud(test.getConnection());
    }

}
