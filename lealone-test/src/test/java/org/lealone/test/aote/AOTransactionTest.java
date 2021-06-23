/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import java.util.HashMap;

import org.lealone.db.RunMode;
import org.lealone.db.async.Future;
import org.lealone.db.value.ValueString;
import org.lealone.server.protocol.dt.DTransactionCommitAck;
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
        public Future<DTransactionCommitAck> commitTransaction(String localTransactionName) {
            return null;
        }

        @Override
        public void commitFinal() {
        }

        @Override
        public void rollbackTransaction() {
        }
    }

    // @Test //TODO 有bug
    public void run() {
        TransactionEngine te = AOTransactionEngineTest.getTransactionEngine(true);
        Storage storage = AOTransactionEngineTest.getStorage();
        Transaction t = te.beginTransaction(false, RunMode.SHARDING);

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
        map = map.getInstance(t);
        assertEquals(null, map.get("3"));
        assertEquals(null, map.get("4"));

        te.close();
    }
}
