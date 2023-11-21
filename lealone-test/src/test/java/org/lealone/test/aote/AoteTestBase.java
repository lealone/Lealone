/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.lealone.db.Constants;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.test.TestBase;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.aote.log.LogSyncService;

public abstract class AoteTestBase extends TestBase implements TestBase.EmbeddedTest {

    protected final String mapName = getClass().getSimpleName();

    protected static TransactionEngine te;
    protected static Storage storage;

    @BeforeClass
    public static void beforeClass() {
        if (te == null) {
            te = getTransactionEngine(); // JVM进程退出时会自动调用ShutdownHook关闭TransactionEngine
            storage = getStorage();
        }
    }

    public static Storage getStorage() {
        StorageEngine se = StorageEngine.getDefaultStorageEngine();
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
        TransactionEngine te = TransactionEngine.getDefaultTransactionEngine();
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
        return getDefaultConfig(null);
    }

    public static Map<String, String> getDefaultConfig(String baseDir) {
        Map<String, String> config = new HashMap<>();
        config.put("base_dir", baseDir != null ? baseDir : joinDirs("aote"));
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_INSTANT);
        // config.put("checkpoint_service_loop_interval", "10"); // 10ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        // config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_NO_SYNC);
        config.put("log_sync_period", "500"); // 500ms
        config.put("embedded", "true");
        return config;
    }
}
