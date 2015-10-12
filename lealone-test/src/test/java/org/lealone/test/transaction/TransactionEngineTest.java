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
import org.lealone.test.UnitTestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;
import org.lealone.transaction.TransactionMap;

public class TransactionEngineTest extends UnitTestBase {
    @Test
    public void run() {
        TransactionEngine te = TransactionEngineManager.getInstance().getEngine(
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_TRANSACTION_ENGINE_NAME, te.getName());

        StorageEngine se = StorageEngineManager.getInstance().getEngine(Constants.DEFAULT_STORAGE_ENGINE_NAME);
        assertEquals(Constants.DEFAULT_STORAGE_ENGINE_NAME, se.getName());

        StorageBuilder storageBuilder = se.getStorageBuilder();
        storageBuilder.storageName(joinDirs("transaction-test", "data"));
        Storage storage = storageBuilder.openStorage();

        Map<String, String> config = new HashMap<>();
        config.put("base_dir", joinDirs("transaction-test"));
        config.put("transaction_log_dir", "log");
        te.init(config);
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap("test", storage);
        map.put("1", "a");
        map.put("2", "b");
        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));

        t.rollback();

        t = te.beginTransaction(false);

        assertNull(map.get("1"));
        assertNull(map.get("2"));
        map = map.getInstance(t, Long.MAX_VALUE);
        map.put("1", "a");
        map.put("2", "b");
        t.commit();

        assertEquals(2, map.sizeAsLong());
    }
}
