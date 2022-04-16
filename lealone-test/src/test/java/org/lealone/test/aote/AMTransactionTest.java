/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.TransactionalValue;

public class AMTransactionTest extends AoteTestBase {
    @Test
    public void run() {
        String mapName = AMTransactionTest.class.getSimpleName();
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map1 = t.openMap(mapName + "1", storage);
        map1.clear();
        map1.put("1", "a");
        map1.put("2", "b");

        TransactionMap<String, String> map2 = t.openMap(mapName + "2", storage);
        map2.clear();
        map2.put("1", "a");
        map2.put("2", "b");

        CountDownLatch latch = new CountDownLatch(1);
        Runnable asyncTask = () -> {
            latch.countDown();
        };
        t.asyncCommit(asyncTask);
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        TransactionalValue tv = (TransactionalValue) map1.getTransactionalValue("1");
        assertTrue(tv.isCommitted());
        tv = (TransactionalValue) map2.getTransactionalValue("1");
        assertTrue(tv.isCommitted());

        t = te.beginTransaction(false);
        map1 = t.openMap(mapName + "1", storage);
        map1.put("3", "c");
        t.commit();
        tv = (TransactionalValue) map1.getTransactionalValue("3");
        assertTrue(tv.isCommitted());
    }
}
