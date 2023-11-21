/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import org.junit.Test;

public class ConcurrentTransactionTest extends AoteTestBase {
    @Test
    public void run() throws Exception {
        Thread t1 = new Thread(() -> {
            new TransactionTest().run();
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            new TransactionTest().run();
        });
        t2.start();
        t1.join();
        t2.join();
    }
}
