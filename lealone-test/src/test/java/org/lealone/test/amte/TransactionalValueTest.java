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

import org.junit.Test;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;
import org.lealone.transaction.amte.TransactionalValue;

public class TransactionalValueTest extends TestBase {

    private TransactionEngine te;
    private Storage storage;

    @Test
    public void run() {
        te = AMTransactionEngineTest.getTransactionEngine();
        storage = AMTransactionEngineTest.getStorage();
        try {
            testExclusiveCommit();
            testExclusiveRollback();
        } finally {
            te.close();
        }
    }

    void testExclusiveCommit() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap("testExclusiveCommit", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.commit();
        // 同一个事务对同一个key更新了多次时只保留最近的一次，并且旧的值为null
        TransactionalValue tv = (TransactionalValue) map.getTransactionalValue("2");
        assertNull(tv.getOldValue());

        t = te.beginTransaction(false);
        map = t.openMap("testExclusiveCommit", storage);
        map.put("2", "b4");
        map.put("2", "b5");
        t.commit();
        tv = (TransactionalValue) map.getTransactionalValue("2");
        assertNotNull(tv.getOldValue());
        assertEquals("b5", tv.getValue());
        assertEquals("b3", tv.getOldValue().getValue());
    }

    void testExclusiveRollback() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.rollback();
        TransactionalValue tv = (TransactionalValue) map.getTransactionalValue("2");
        assertNull(tv);

        t = te.beginTransaction(false);
        map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        t.addSavepoint("sp1");
        map.put("2", "b3");
        t.rollbackToSavepoint("sp1");
        t.commit();
        tv = (TransactionalValue) map.getTransactionalValue("2");
        assertEquals("b2", tv.getValue());
    }
}
