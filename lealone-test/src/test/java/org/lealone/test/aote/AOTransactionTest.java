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

import java.util.HashMap;

import org.junit.Test;
import org.lealone.db.RunMode;
import org.lealone.db.value.ValueString;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class AOTransactionTest extends TestBase {

    private static class ParticipantTest implements Transaction.Participant {
        @Override
        public void addSavepoint(String name) {
        }

        @Override
        public void rollbackToSavepoint(String name) {
        }

        @Override
        public void commitTransaction(String localTransactionName) {
        }

        @Override
        public void rollbackTransaction() {
        }
    }

    @Test
    public void run() {
        TransactionEngine te = AOTransactionEngineTest.getTransactionEngine(true);
        Storage storage = AOTransactionEngineTest.getStorage();
        Transaction t = te.beginTransaction(false, RunMode.SHARDING);
        t.setLocal(false);

        ParticipantTest p1 = new ParticipantTest();
        ParticipantTest p2 = new ParticipantTest();

        t.addParticipant(p1);
        t.addParticipant(p2);

        long tid1 = t.getTransactionId() + 2;
        long tid2 = t.getTransactionId() + 4;

        String alocalTransactionName1 = "127.0.0.1:9210:" + tid1;
        String alocalTransactionName2 = "127.0.0.1:9210:" + tid2;

        t.addLocalTransactionNames(alocalTransactionName1);
        t.addLocalTransactionNames(alocalTransactionName2);

        HashMap<String, String> parameters = new HashMap<>(1);
        parameters.put("initReplicationNodes", "127.0.0.1:9210");
        TransactionMap<String, String> map = t.openMap("DistributedTransactionTest", ValueString.type, ValueString.type,
                storage, parameters);
        map.clear();
        map.put("1", "a");
        map.put("2", "b");
        t.commit();

        t = te.beginTransaction(false, RunMode.SHARDING);
        t.setLocal(false);
        map = map.getInstance(t);
        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));

        t.addParticipant(p1);
        t.addParticipant(p2);
        t.addLocalTransactionNames(alocalTransactionName1);
        t.addLocalTransactionNames(alocalTransactionName2);
        map.put("3", "c");
        map.put("4", "d");
        t.commit();

        t = te.beginTransaction(false, RunMode.SHARDING);
        t.setLocal(false);
        map = map.getInstance(t);
        assertEquals(null, map.get("3"));
        assertEquals(null, map.get("4"));

        te.close();
    }
}
