/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.lealone.storage.Storage;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.AOTransactionEngine;
import org.lealone.transaction.aote.log.LogSyncService;

public class TransactionEngineTest extends AoteTestBase {

    private static TransactionEngine te;
    private static Storage storage;

    @BeforeClass
    public static void beforeClass() { // 不会触发父类的before
        Map<String, String> config = getDefaultConfig(joinDirs("aote", "TransactionEngineTest"));
        config.put("dirty_page_cache_size_in_mb", "1");
        config.put("checkpoint_service_loop_interval", "100"); // 100ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);

        // 避免覆盖存在的TransactionEngine
        config.put("plugin_name", "TransactionEngineTest");
        te = new AOTransactionEngine();
        te.init(config);
        storage = getStorage();
    }

    @AfterClass
    public static void afterClass() {
        te.close();
    }

    @Test
    public void testEngine() {
        Transaction t1 = te.beginTransaction(false);
        t1.openMap("testEngine", storage);
        assertNotNull(te.getTransactionMap("testEngine", t1));
        storage.close();
        assertNull(te.getTransactionMap("testEngine", t1));
    }

    @Test
    public void testCheckpoint() {
        // 1. 超过脏页大小时自动执行一次检查点
        Transaction t1 = te.beginTransaction(false);
        TransactionMap<String, String> map = t1.openMap("testCheckpoint", storage);
        map.remove();
        map = t1.openMap("testCheckpoint", storage);
        assertEquals(0, map.getDiskSpaceUsed());
        assertEquals(0, map.size());

        for (int i = 1; i <= 50000; i++) {
            map.put("key" + i, "value" + i);
        }
        t1.commit();
        assertEquals(50000, map.size());

        sleep(map);
        assertTrue(map.getDiskSpaceUsed() > 0);

        // 2. 手动执行检查点
        map.remove();
        Transaction t2 = te.beginTransaction(false);
        map = t2.openMap("testCheckpoint", storage);
        assertEquals(0, map.getDiskSpaceUsed());
        map.put("abc", "value123");
        t2.commit();

        te.checkpoint();

        sleep(map);
        assertTrue(map.getDiskSpaceUsed() > 0);
    }

    private void sleep(TransactionMap<String, String> map) {
        long sleep = 0;
        while (map.getDiskSpaceUsed() <= 0 || sleep > 3000) {
            try {
                Thread.sleep(100); // 等待后端检查点线程完成数据保存
            } catch (InterruptedException e) {
            }
            sleep += 100;
        }
    }
}
