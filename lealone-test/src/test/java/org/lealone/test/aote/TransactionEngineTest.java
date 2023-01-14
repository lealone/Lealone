/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.db.PluginManager;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.AOTransactionEngine;
import org.lealone.transaction.aote.log.LogSyncService;

public class TransactionEngineTest extends AoteTestBase {

    public static Storage getStorage() {
        StorageEngine se = PluginManager.getPlugin(StorageEngine.class,
                Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storagePath(joinDirs("aote", "data"));
        Storage storage = storageBuilder.openStorage();
        return storage;
    }

    public static TransactionEngine getTransactionEngine() {
        return getTransactionEngine(null, false);
    }

    public static TransactionEngine getTransactionEngine(boolean isDistributed) {
        return getTransactionEngine(null, isDistributed);
    }

    public static TransactionEngine getTransactionEngine(Map<String, String> config) {
        return getTransactionEngine(config, false);
    }

    public static TransactionEngine getTransactionEngine(Map<String, String> config,
            boolean isDistributed) {
        TransactionEngine te = PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_TRANSACTION_ENGINE_NAME, te.getName());
        if (config == null)
            config = getDefaultConfig();
        if (isDistributed) {
            config.put("host_and_port", Constants.DEFAULT_HOST + ":" + Constants.DEFAULT_TCP_PORT);
        }
        te.init(config);
        return te;
    }

    public static Map<String, String> getDefaultConfig() {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", joinDirs("aote"));
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_INSTANT);
        // config.put("checkpoint_service_loop_interval", "10"); // 10ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        // config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_NO_SYNC);
        config.put("log_sync_period", "500"); // 500ms
        config.put("embedded", "true");
        return config;
    }

    @Before
    @Override
    public void before() {
    }

    @Test
    public void testEngine() {
        te = new AOTransactionEngine();
        te.init(getDefaultConfig());
        storage = getStorage();

        Transaction t1 = te.beginTransaction(false);
        t1.openMap("testEngine", storage);
        assertNotNull(te.getTransactionMap("testEngine", t1));
        storage.close();
        assertNull(te.getTransactionMap("testEngine", t1));
    }

    @Test
    public void testCheckpoint() {
        Map<String, String> config = getDefaultConfig();
        config.put("committed_data_cache_size_in_mb", "1");
        config.put("checkpoint_service_loop_interval", "100"); // 100ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        te = getTransactionEngine(config);
        storage = getStorage();

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

        // 在进行集成测试时可能在其他地方已经初始化TransactionEngine了
        String v = te.getConfig().get("committed_data_cache_size_in_mb");
        if (v != null && v.equals("1")) {
            try {
                Thread.sleep(2000); // 等待后端检查点线程完成数据保存
            } catch (InterruptedException e) {
            }
        } else {
            te.checkpoint();
        }
        assertTrue(map.getDiskSpaceUsed() > 0);

        map.remove();
        Transaction t2 = te.beginTransaction(false);
        map = t2.openMap("testCheckpoint", storage);
        assertEquals(0, map.getDiskSpaceUsed());
        map.put("abc", "value123");
        t2.commit();

        te.checkpoint();
        assertTrue(map.getDiskSpaceUsed() > 0);
    }
}
