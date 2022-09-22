/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import org.junit.After;
import org.junit.Before;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.transaction.TransactionEngine;

public abstract class AoteTestBase extends TestBase implements TestBase.EmbeddedTest {

    protected TransactionEngine te;
    protected Storage storage;

    @Before
    public void before() {
        te = TransactionEngineTest.getTransactionEngine();
        storage = TransactionEngineTest.getStorage();
    }

    @After
    public void after() {
        te.close();
    }
}
