/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aote;

import org.junit.Test;

import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;

public class TransactionCommitTest extends AoteTestBase {
    @Test
    public void run() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap(mapName, storage);
        map.put("2", "b");
        map.put("3", "c");
        map.remove();
        t.commit();

        t = te.beginTransaction();
        map = t.openMap(mapName, storage);
        map.put("4", "b4");
        map.put("5", "c5");
        t.asyncCommit();
    }
}
