/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.aote;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.lealone.db.index.standard.RowType;
import com.lealone.db.result.Row;
import com.lealone.db.result.SortOrder;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueInt;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.TransactionMap;
import com.lealone.transaction.aote.TransactionalValue;

public class TransactionalValueTest extends AoteTestBase {

    @Test
    public void run() {
        testExclusiveCommit();
        testExclusiveRollback();
        testUncommittedCommit();
        testRemove();
    }

    void testExclusiveCommit() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testExclusiveCommit", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.commit();

        t = te.beginTransaction();
        map = t.openMap("testExclusiveCommit", storage);
        map.put("2", "b4");
        map.put("2", "b5");
        t.commit();
        TransactionalValue tv = (TransactionalValue) map.getLockableValue("2");
        assertEquals("b5", tv.getValue());
    }

    void testExclusiveRollback() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        map.put("2", "b3");
        t.rollback();
        TransactionalValue tv = (TransactionalValue) map.getLockableValue("2");
        assertNull(tv);

        t = te.beginTransaction();
        map = t.openMap("testExclusiveRollback", storage);
        map.clear();
        map.put("2", "b1");
        map.put("2", "b2");
        t.addSavepoint("sp1");
        map.put("2", "b3");
        t.rollbackToSavepoint("sp1");
        t.commit();
        tv = (TransactionalValue) map.getLockableValue("2");
        assertEquals("b2", tv.getValue());
    }

    void testUncommittedCommit() {
        String mapName = "testUncommittedCommit";
        int columns = 4;
        int[] sortTypes = new int[columns];
        for (int i = 0; i < columns; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        RowType type = new RowType(sortTypes, columns);

        Transaction t = te.beginTransaction();
        TransactionMap<String, Row> map = t.openMap(mapName, null, type, storage);
        map.clear();

        String key = "1";

        ValueArray valueArray = createValueArray(0, 0, 0, 0);
        Row r = new Row(1, valueArray.getList());
        map.put(key, r);
        t.commit();

        Transaction t1 = te.beginTransaction();
        TransactionMap<String, Row> map1 = t1.openMap(mapName, storage);

        Transaction t2 = te.beginTransaction();
        TransactionMap<String, Row> map2 = t2.openMap(mapName, storage);

        Transaction t3 = te.beginTransaction();
        TransactionMap<String, Row> map3 = t3.openMap(mapName, storage);

        r = createRow(map1, key, 0, 10);
        map1.tryUpdate(key, r);
        r = createRow(map1, key, 0, 11);
        map1.tryUpdate(key, r);

        r = createRow(map2, key, 1, 20);
        map2.tryUpdate(key, r);
        r = createRow(map2, key, 1, 21);
        map2.tryUpdate(key, r);

        r = createRow(map3, key, 2, 30);
        map3.tryUpdate(key, r);
        r = createRow(map3, key, 2, 31);
        map3.tryUpdate(key, r);

        r = (Row) map3.getLockableValue(key);
        System.out.println(r);
        System.out.println("========");

        // t2.commit();
        // // t2.rollback();
        // t3.commit();
        // t1.commit();

        CountDownLatch latch = new CountDownLatch(3);
        new Thread(() -> {
            t2.commit();
            latch.countDown();
        }).start();
        new Thread(() -> {
            t3.commit();
            latch.countDown();
        }).start();
        new Thread(() -> {
            t1.commit();
            latch.countDown();
        }).start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("========");
        r = (Row) map3.getLockableValue(key);
        System.out.println(r);
    }

    private ValueArray createValueArray(int... values) {
        ValueInt[] a = new ValueInt[values.length];
        for (int i = 0; i < a.length; i++)
            a[i] = ValueInt.get(values[i]);
        return ValueArray.get(a);
    }

    private Row createRow(TransactionMap<String, Row> map, String key, int columnIndex, int value) {
        Row vv = map.get(key);
        Value[] values = vv.getColumns().clone();
        values[columnIndex] = ValueInt.get(value);
        vv = new Row(1, values);
        return vv;
    }

    void testRemove() {
        Transaction t = te.beginTransaction();
        TransactionMap<String, String> map = t.openMap("testRemove", storage);
        map.clear();
        map.put("2", "b1");
        t.commit();

        Transaction t1 = te.beginTransaction(Transaction.IL_REPEATABLE_READ);
        TransactionMap<String, String> map1 = t1.openMap("testRemove", storage);

        Transaction t2 = te.beginTransaction();
        TransactionMap<String, String> map2 = t2.openMap("testRemove", storage);
        map2.remove("2");
        t2.commit();

        String v = map1.get("2");
        System.out.println(v);
        t1.commit();
    }
}
