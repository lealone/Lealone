/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aote;

import org.junit.Test;
import org.lealone.db.index.standard.ValueDataType;
import org.lealone.db.index.standard.VersionedValue;
import org.lealone.db.index.standard.VersionedValueType;
import org.lealone.db.result.SortOrder;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueInt;
import org.lealone.storage.Storage;
import org.lealone.storage.type.ObjectDataType;
import org.lealone.test.TestBase;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class AMTransactionMapTest extends TestBase {

    private TransactionEngine te;
    private Storage storage;

    @Test
    public void run() {
        te = AMTransactionEngineTest.getTransactionEngine();
        storage = AMTransactionEngineTest.getStorage();

        testSyncOperations();
        testTryOperations();
        testColumnLock();

        te.close();
    }

    private String createMapName(String name) {
        return AMTransactionMapTest.class.getSimpleName() + "-" + name;
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
        t5.asyncCommit(); // 提交到后台，由LogSyncService线程在sync完事务日志后自动提交事务
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        assertEquals(Transaction.STATUS_CLOSED, t5.getStatus());
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

    void testColumnLock() {
        String mapName = createMapName("testColumnLock");
        int columns = 4;
        int[] sortTypes = new int[columns];
        for (int i = 0; i < columns; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType valueType = new ValueDataType(null, null, sortTypes);
        VersionedValueType vvType = new VersionedValueType(valueType, columns);

        Transaction t = te.beginTransaction(false);
        TransactionMap<String, VersionedValue> map = t.openMap(mapName, null, vvType, storage);
        map.clear();

        String key = "1";

        Object oldValue = map.get(key);
        ValueArray valueArray = createValueArray(0, 0, 0, 0);
        VersionedValue vv = new VersionedValue(1, valueArray);
        map.addIfAbsent(key, vv).get();
        t.commit();

        // int[] columnIndexes1 = { 0 };
        Transaction t1 = te.beginTransaction(false);
        TransactionMap<String, VersionedValue> map1 = t1.openMap(mapName, storage);
        // int[] columnIndexes2 = { 1 };
        Transaction t2 = te.beginTransaction(false);
        TransactionMap<String, VersionedValue> map2 = t2.openMap(mapName, storage);

        Transaction t3 = te.beginTransaction(false);
        TransactionMap<String, VersionedValue> map3 = t3.openMap(mapName, storage);

        // vv = map1.get(key);
        // valueArray = vv.value;
        // valueArray.getList()[0] = ValueInt.get(11);

        int columnIndex;
        boolean ok;
        oldValue = map1.getTransactionalValue(key);
        columnIndex = 0;
        vv = createVersionedValue(map1, key, columnIndex, 1);
        ok = map1.tryUpdate(key, vv, new int[] { columnIndex }, oldValue) == Transaction.OPERATION_COMPLETE;
        assertTrue(ok);

        oldValue = map2.getTransactionalValue(key);
        columnIndex = 1;
        vv = createVersionedValue(map2, key, columnIndex, 1);
        ok = map2.tryUpdate(key, vv, new int[] { columnIndex }, oldValue) == Transaction.OPERATION_COMPLETE;
        assertTrue(ok);

        oldValue = map1.getTransactionalValue(key);
        columnIndex = 2;
        vv = createVersionedValue(map1, key, columnIndex, 1);
        ok = map1.tryUpdate(key, vv, new int[] { columnIndex }, oldValue) == Transaction.OPERATION_COMPLETE;
        assertTrue(ok);

        oldValue = map2.getTransactionalValue(key);
        columnIndex = 3;
        vv = createVersionedValue(map2, key, columnIndex, 1);
        ok = map2.tryUpdate(key, vv, new int[] { columnIndex }, oldValue) == Transaction.OPERATION_COMPLETE;
        assertTrue(ok);

        oldValue = map3.getTransactionalValue(key);
        columnIndex = 3;
        vv = createVersionedValue(map3, key, columnIndex, 1);
        ok = map3.tryUpdate(key, vv, new int[] { columnIndex }, oldValue) != Transaction.OPERATION_COMPLETE;
        assertTrue(ok);

        t2.rollback();
        // t2.commit();
        t1.commit();
    }

    private ValueArray createValueArray(int... values) {
        ValueInt[] a = new ValueInt[values.length];
        for (int i = 0; i < a.length; i++)
            a[i] = ValueInt.get(values[i]);
        return ValueArray.get(a);
    }

    private VersionedValue createVersionedValue(TransactionMap<String, VersionedValue> map, String key, int columnIndex,
            int value) {
        VersionedValue vv = map.get(key);
        Value[] values = vv.value.getList().clone();
        values[columnIndex] = ValueInt.get(value);
        ValueArray valueArray = ValueArray.get(values);
        vv = new VersionedValue(1, valueArray);
        return vv;
    }
}
