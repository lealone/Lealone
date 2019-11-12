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
package org.lealone.test.aote;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.aote.TransactionalValue;

public class AMTransactionTest extends TestBase {
    @Test
    public void run() {
        TransactionEngine te = AMTransactionEngineTest.getTransactionEngine();
        Storage storage = AMTransactionEngineTest.getStorage();
        String mapName = AMTransactionTest.class.getSimpleName();
        try {
            Transaction t = te.beginTransaction(false);
            TransactionMap<String, String> map1 = t.openMap(mapName + "1", storage);
            map1.clear();
            map1.put("1", "a");
            map1.put("2", "b");

            TransactionMap<String, String> map2 = t.openMap(mapName + "2", storage);
            map2.clear();
            map2.put("1", "a");
            map2.put("2", "b");

            CountDownLatch latch = new CountDownLatch(1);
            Runnable asyncTask = () -> {
                latch.countDown();
            };
            t.asyncCommit(asyncTask);
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            TransactionalValue tv = (TransactionalValue) map1.getTransactionalValue("1");
            assertTrue(tv.isCommitted());
            tv = (TransactionalValue) map2.getTransactionalValue("1");
            assertTrue(tv.isCommitted());

            t = te.beginTransaction(false);
            map1 = t.openMap(mapName + "1", storage);
            map1.put("3", "c");
            t.commit();
            tv = (TransactionalValue) map1.getTransactionalValue("3");
            assertTrue(tv.isCommitted());
        } finally {
            te.close();
        }
    }
}
