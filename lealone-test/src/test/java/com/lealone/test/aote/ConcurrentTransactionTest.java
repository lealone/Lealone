/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aote;

import org.junit.Test;

import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;

public class ConcurrentTransactionTest extends AoteTestBase {
    @Test
    public void run() throws Exception {
        Thread t1 = new Thread(() -> {
            testSyncOperations();
            testAsyncOperations();
        });
        t1.start();
        Thread t2 = new Thread(() -> {
            testSyncOperations();
            testAsyncOperations();
        });
        t2.start();
        t1.join();
        t2.join();
    }

    void testSyncOperations() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map1 = t.openMap(mapName + "1", storage);
        TransactionMap<String, String> map2 = t.openMap(mapName + "2", storage);
        map1.put("1", "a");
        map2.put("2", "b");
        map1.put("3", "c");
        map2.put("4", "d");
        t.commit();
    }

    void testAsyncOperations() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map1 = t.openMap(mapName + "1", storage);
        map1.put("1", "a", ar -> {
            System.out.println("old: " + ar.getResult());
        });
        map1.put("2", "b", ar -> {
            System.out.println("old: " + ar.getResult());
        });
        t.asyncCommit(() -> {
            System.out.println("async committed");
        });
    }
}
