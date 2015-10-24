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

import org.junit.Test;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class DistributedTransactionTest extends TestBase {

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

    private static class ValidatorTest implements Transaction.Validator {
        private final boolean validateResult;

        public ValidatorTest(boolean validateResult) {
            this.validateResult = validateResult;
        }

        @Override
        public boolean validateTransaction(String localTransactionName) {
            return validateResult;
        }
    }

    @Test
    public void run() {
        TransactionEngine te = TransactionEngineTest.getTransactionEngine(true);
        Storage storage = TransactionEngineTest.getStorage();

        Transaction t = te.beginTransaction(false);
        t.setLocal(false);

        ParticipantTest p1 = new ParticipantTest();
        ParticipantTest p2 = new ParticipantTest();
        ValidatorTest validatorTrue = new ValidatorTest(true);
        ValidatorTest validatorFalse = new ValidatorTest(false);

        t.addParticipant(p1);
        t.addParticipant(p2);
        t.setValidator(validatorTrue);

        long tid1 = t.getTransactionId() + 2;
        long tid2 = t.getTransactionId() + 4;

        String alocalTransactionName1 = "127.0.0.1:5210:" + tid1;
        String alocalTransactionName2 = "127.0.0.1:5210:" + tid2;

        t.addLocalTransactionNames(alocalTransactionName1);
        t.addLocalTransactionNames(alocalTransactionName2);

        TransactionMap<String, String> map = t.openMap("DistributedTransactionTest", storage);
        map.clear();
        map.put("1", "a");
        map.put("2", "b");
        t.commit();

        t = te.beginTransaction(false);
        t.setLocal(false);
        t.setValidator(validatorTrue);
        map = map.getInstance(t);
        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));

        t.addParticipant(p1);
        t.addParticipant(p2);
        t.setValidator(validatorFalse);
        t.addLocalTransactionNames(alocalTransactionName1);
        t.addLocalTransactionNames(alocalTransactionName2);
        map.put("3", "c");
        map.put("4", "d");
        t.commit();

        t = te.beginTransaction(false);
        t.setLocal(false);
        t.setValidator(validatorFalse);
        map = map.getInstance(t);
        assertEquals(null, map.get("3"));
        assertEquals(null, map.get("4"));

        te.close();
    }
}
