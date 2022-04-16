/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.Map;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.db.PluginManager;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class AOTransactionEngineTest extends AoteTestBase {

    public static TransactionEngine getTransactionEngine(boolean isDistributed) {
        TransactionEngine te = PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_TRANSACTION_ENGINE_NAME, te.getName());

        Map<String, String> config = AMTransactionEngineTest.getDefaultConfig();

        if (isDistributed) {
            config.put("host_and_port", Constants.DEFAULT_HOST + ":" + Constants.DEFAULT_TCP_PORT);
        }
        te.init(config);
        return te;
    }

    public static Storage getStorage() {
        StorageEngine se = PluginManager.getPlugin(StorageEngine.class, Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storagePath(joinDirs("aote", "data"));
        Storage storage = storageBuilder.openStorage();
        return storage;
    }

    @Test
    public void run() {
        TransactionEngine te = getTransactionEngine(false);
        Storage storage = getStorage();

        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap("test", storage);
        map.clear();
        map.put("1", "a");
        map.put("2", "b");
        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));

        t.rollback();
        try {
            map.put("1", "a"); // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            fail();
        } catch (IllegalStateException e) {
        }

        t = te.beginTransaction(false);
        map = map.getInstance(t);

        assertNull(map.get("1"));
        assertNull(map.get("2"));
        map.put("1", "a");
        map.put("2", "b");
        t.commit();

        map.get("1"); // 虽然事务commit后就自动关闭了，但是读操作还是允许的

        try {
            map.put("1", "a"); // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(2, map.size());

        Transaction t2 = te.beginTransaction(false);
        map = map.getInstance(t2);
        map.put("3", "c");
        map.put("4", "d");
        assertEquals(4, map.size());

        Transaction t3 = te.beginTransaction(false);
        map = map.getInstance(t3);
        map.put("5", "f");
        assertEquals(3, map.size());

        Transaction t4 = te.beginTransaction(false);
        map = map.getInstance(t4);
        map.remove("1");
        assertEquals(1, map.size());
        t4.commit();

        te.close();
    }
}
