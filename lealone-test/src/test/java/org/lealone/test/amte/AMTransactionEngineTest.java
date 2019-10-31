/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.amte;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.lealone.db.Constants;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageBuilder;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.amte.AMTransactionEngine;
import org.lealone.transaction.amte.log.LogSyncService;

public class AMTransactionEngineTest extends TestBase {

    public static Storage getStorage() {
        StorageEngine se = StorageEngineManager.getStorageEngine(Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storagePath(joinDirs("amte", "data"));
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

    public static TransactionEngine getTransactionEngine(Map<String, String> config, boolean isDistributed) {
        TransactionEngine te = TransactionEngineManager.getTransactionEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
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
        config.put("base_dir", joinDirs("amte"));
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_INSTANT);
        // config.put("checkpoint_service_loop_interval", "10"); // 10ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        // config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_NO_SYNC);
        config.put("log_sync_period", "500"); // 500ms
        return config;
    }

    @Test
    public void testEngine() {
        AMTransactionEngine te = new AMTransactionEngine();
        try {
            te.beginTransaction(true);
            fail();
        } catch (Exception e) {
        }

        te.init(getDefaultConfig());
        Storage storage = getStorage();

        Transaction t1 = te.beginTransaction(false);
        t1.openMap("testEngine", storage);
        assertNotNull(te.getTransactionMap("testEngine", t1));
        storage.close();
        assertNull(te.getTransactionMap("testEngine", t1));
        te.close();
    }

    @Test
    public void testCheckpoint() {
        Map<String, String> config = getDefaultConfig();
        config.put("committed_data_cache_size_in_mb", "1");
        config.put("checkpoint_service_loop_interval", "100"); // 100ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        TransactionEngine te = getTransactionEngine(config);
        Storage storage = getStorage();

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
        try {
            Thread.sleep(2000); // 等待后端检查点线程完成数据保存
        } catch (InterruptedException e) {
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
        te.close();
    }
}
