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
package org.lealone.test.transaction;

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

public class TransactionEngineTest extends TestBase {

    public static TransactionEngine getTransactionEngine(boolean isDistributed) {
        TransactionEngine te = TransactionEngineManager.getInstance().getEngine(
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_TRANSACTION_ENGINE_NAME, te.getName());

        Map<String, String> config = new HashMap<>();
        config.put("base_dir", joinDirs("transaction-test"));
        config.put("transaction_log_dir", "tlog");
        config.put("log_sync_type", "batch");
        config.put("log_sync_batch_window", "10"); // 10ms

        // config.put("log_sync_type", "periodic");
        // config.put("log_sync_period", "500"); // 500ms

        if (isDistributed) {
            config.put("is_cluster_mode", "true");
            config.put("host_and_port", Constants.DEFAULT_HOST + ":" + Constants.DEFAULT_TCP_PORT);
        }

        te.init(config);

        return te;
    }

    public static Storage getStorage() {
        StorageEngine se = StorageEngineManager.getInstance().getEngine(Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storageName(joinDirs("transaction-test", "data"));
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
