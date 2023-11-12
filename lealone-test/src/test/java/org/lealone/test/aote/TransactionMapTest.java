/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import org.junit.Test;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

public class TransactionMapTest extends AoteTestBase {
    @Test
    public void run() {
        testSyncOperations();
        testTryOperations();
    }

    private String createMapName(String name) {
        return TransactionMapTest.class.getSimpleName() + "-" + name;
    }

    void testSyncOperations() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap(createMapName("testSyncOperations"), storage);
        map.clear();
        assertTrue(map.getValueType() instanceof ObjectDataType);

        map.put("1", "a");
        map.put("2", "b");

        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));
        assertEquals(2, map.size());

        t.rollback();
        assertEquals(0, map.size());
        try {
            // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            map.put("1", "a");
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
            // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            map.put("1", "a");
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
        assertEquals(3, map.size()); // t2未提交，所以读不到它put的数据

        Transaction t4 = te.beginTransaction(false);
        map = map.getInstance(t4);
        map.remove("1");
        assertEquals(1, map.size());
        t4.commit();

        Transaction t5 = te.beginTransaction(false);
        map = map.getInstance(t5);
        map.put("6", "g");
        assertEquals(2, map.size());

        // 提交到后台，由LogSyncService线程在sync完事务日志后自动提交事务
        t5.asyncCommit(() -> {
            assertTrue(t5.isClosed());
        });
    }

    void testTryOperations() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<String, String> map = t.openMap(createMapName("testTryOperations"), storage);
        map.clear();

        map.addIfAbsent("1", "a").get();
        t.commit();
        assertNotNull(map.get("1"));

        Transaction t2 = te.beginTransaction(false);
        map = map.getInstance(t2);
        assertTrue(map.tryUpdate("1", "a2") == Transaction.OPERATION_COMPLETE);
        assertEquals("a2", map.get("1"));

        Transaction t3 = te.beginTransaction(false);
        map = map.getInstance(t3);
        assertTrue(map.tryUpdate("1", "a3") == Transaction.OPERATION_NEED_WAIT);
        assertEquals("a", map.get("1"));

        t2.commit();
        t3.rollback();

        Transaction t4 = te.beginTransaction(false);
        map = map.getInstance(t4);
        assertTrue(map.tryRemove("1") == Transaction.OPERATION_COMPLETE);

        Transaction t5 = te.beginTransaction(false);
        map = map.getInstance(t5);
        assertTrue(map.tryRemove("1") == Transaction.OPERATION_NEED_WAIT);

        t4.commit();
        t5.rollback();
        assertNull(map.get("1"));
    }
}
