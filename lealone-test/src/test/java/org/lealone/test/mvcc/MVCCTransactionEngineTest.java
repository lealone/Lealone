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
package org.lealone.test.mvcc;

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
import org.lealone.transaction.mvcc.log.LogSyncService;

public class MVCCTransactionEngineTest extends TestBase {

    public static Storage getStorage() {
        StorageEngine se = StorageEngineManager.getInstance().getEngine(Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storagePath(joinDirs("mvcc", "data"));
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
        TransactionEngine te = TransactionEngineManager.getInstance()
                .getEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
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
        config.put("base_dir", joinDirs("mvcc"));
        config.put("redo_log_dir", "redo_log");
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_BATCH);
        config.put("log_sync_batch_window", "10"); // 10ms
        // config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        // config.put("log_sync_period", "500"); // 500ms
        return config;
    }

    @Test
    public void testCheckpoint() {
        Map<String, String> config = getDefaultConfig();
        config.put("committed_data_cache_size_in_mb", "1");
        config.put("checkpoint_service_sleep_interval", "100"); // 100ms
        config.put("log_sync_type", LogSyncService.LOG_SYNC_TYPE_PERIODIC);
        TransactionEngine te = getTransactionEngine(config);
        Storage storage = getStorage();

        Transaction t1 = te.beginTransaction(false, false);
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
        Transaction t2 = te.beginTransaction(false, false);
        map = t2.openMap("testCheckpoint", storage);
        assertEquals(0, map.getDiskSpaceUsed());
        map.put("abc", "value123");
        t2.commit();

        te.checkpoint();
        assertTrue(map.getDiskSpaceUsed() > 0);
    }

    @Test
    public void run() {
        TransactionEngine te = getTransactionEngine(false);
        Storage storage = getStorage();

        Transaction t = te.beginTransaction(false, false);
        TransactionMap<String, String> map = t.openMap("test", storage);
        map.clear();
        map.put("1", "a");
        map.put("2", "b");

        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));
        assertEquals(2, map.size());

        t.rollback();
        assertEquals(0, map.size());
        try {
            map.put("1", "a"); // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            fail();
        } catch (IllegalStateException e) {
        }

        t = te.beginTransaction(false, false);
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

        Transaction t2 = te.beginTransaction(false, false);
        map = map.getInstance(t2);
        map.put("3", "c");
        map.put("4", "d");
        assertEquals(4, map.size());

        Transaction t3 = te.beginTransaction(false, false);
        map = map.getInstance(t3);
        map.put("5", "f");
        assertEquals(3, map.size()); // t2未提交，所以读不到它put的数据

        Transaction t4 = te.beginTransaction(false, false);
        map = map.getInstance(t4);
        map.remove("1");
        assertEquals(1, map.size());
        t4.commit();

        Transaction t5 = te.beginTransaction(false, false);
        map = map.getInstance(t5);
        map.put("6", "g");
        assertEquals(2, map.size());
        t5.prepareCommit(); // 提交到后台，由LogSyncService线程在sync完事务日志后自动提交事务
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        assertEquals(Transaction.STATUS_CLOSED, t5.getStatus());

        te.close();
    }
}
