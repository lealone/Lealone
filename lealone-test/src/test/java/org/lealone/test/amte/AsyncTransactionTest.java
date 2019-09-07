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

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.db.index.ValueDataType;
import org.lealone.db.index.VersionedValue;
import org.lealone.db.index.VersionedValueType;
import org.lealone.db.result.SortOrder;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueInt;
import org.lealone.storage.Storage;
import org.lealone.test.TestBase;
import org.lealone.test.perf.PageOperationHandlerImpl;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

public class AsyncTransactionTest extends TestBase {

    class MyTransactionListener implements Transaction.Listener {

        CountDownLatch latch;

        public MyTransactionListener() {

        }

        public MyTransactionListener(CountDownLatch latch) {
            this.latch = latch;
        }

        public MyTransactionListener(int count) {
            this.latch = new CountDownLatch(count);
        }

        @Override
        public void partialUndo() {
            System.out.println("partialUndo");
            if (latch != null)
                latch.countDown();
        }

        @Override
        public void partialComplete() {
            System.out.println("partialComplete");
            if (latch != null)
                latch.countDown();
        }

        public void await() {
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ValueArray createValueArray(int... values) {
        ValueInt[] a = new ValueInt[values.length];
        for (int i = 0; i < a.length; i++)
            a[i] = ValueInt.get(values[i]);
        return ValueArray.get(a);
    }

    @Test
    public void run() {
        PageOperationHandlerImpl.setPageOperationHandlersCount(3);
        PageOperationHandlerImpl.startPageOperationHandlers(null);
        TransactionEngine te = AMTransactionEngineTest.getTransactionEngine(false);
        Storage storage = AMTransactionEngineTest.getStorage();

        // testAsyncRemove(te, storage);
        // testAsyncInsert(te, storage);

        testAsyncMultiTransaction(te, storage);

        te.close();
    }

    public void testAsyncInsert(TransactionEngine te, Storage storage) {
        Transaction t = te.beginTransaction(false, false);
        TransactionMap<String, String> map = t.openMap("AsyncTransactionTest", storage);
        map.clear();

        MyTransactionListener listener = new MyTransactionListener();
        map.put("1", null, "a", null, listener);

        Object oldValue = map.getTransactionalValue("1");
        map.put("1", oldValue, "a1", null, listener);

        oldValue = map.getTransactionalValue("1");
        map.remove("1", oldValue, listener);
        // t.rollback();
        t.commit();

        oldValue = map.get("1");
    }

    public void testAsyncRemove(TransactionEngine te, Storage storage) {
        Transaction t = te.beginTransaction(false, false);
        TransactionMap<String, String> map = t.openMap("AsyncTransactionTest", storage);
        map.clear();

        MyTransactionListener listener = new MyTransactionListener();
        Object oldValue = map.get("1");
        map.put("1", oldValue, "a", null, listener);
        oldValue = map.get("2");
        map.put("2", oldValue, "b", null, listener);

        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));
        assertEquals(2, map.size());
        t.commit();

        t = te.beginTransaction(false, false);
        map = map.getInstance(t);
        oldValue = map.getTransactionalValue("1");
        map.remove("1", oldValue, listener);
        t.commit();
    }

    // @Test
    public void testAsyncMultiTransaction(TransactionEngine te, Storage storage) {
        int columns = 4;
        int[] sortTypes = new int[columns];
        for (int i = 0; i < columns; i++) {
            sortTypes[i] = SortOrder.ASCENDING;
        }
        ValueDataType valueType = new ValueDataType(null, null, sortTypes);
        VersionedValueType vvType = new VersionedValueType(valueType, columns);

        Transaction t = te.beginTransaction(false, false);
        TransactionMap<String, VersionedValue> map = t.openMap("AsyncTransactionTest", null, vvType, storage);
        map.clear();

        String key = "1";

        MyTransactionListener listener = new MyTransactionListener(1);
        Object oldValue = map.get(key);
        ValueArray valueArray = createValueArray(0, 0, 0, 0);
        VersionedValue vv = new VersionedValue(1, valueArray);
        map.put(key, null, vv, null, listener);
        listener.await();
        t.commit();

        // int[] columnIndexes1 = { 0 };
        Transaction t1 = te.beginTransaction(false, false);
        TransactionMap<String, VersionedValue> map1 = t1.openMap("AsyncTransactionTest", storage);
        // int[] columnIndexes2 = { 1 };
        Transaction t2 = te.beginTransaction(false, false);
        TransactionMap<String, VersionedValue> map2 = t2.openMap("AsyncTransactionTest", storage);

        // vv = map1.get(key);
        // valueArray = vv.value;
        // valueArray.getList()[0] = ValueInt.get(11);

        int columnIndex;
        oldValue = map1.getTransactionalValue(key);
        columnIndex = 0;
        vv = createVersionedValue(map1, key, columnIndex, 1);
        map1.put(key, oldValue, vv, new int[] { columnIndex }, null);

        oldValue = map2.getTransactionalValue(key);
        columnIndex = 1;
        vv = createVersionedValue(map2, key, columnIndex, 1);
        map2.put(key, oldValue, vv, new int[] { columnIndex }, null);

        oldValue = map1.getTransactionalValue(key);
        columnIndex = 2;
        vv = createVersionedValue(map1, key, columnIndex, 1);
        map1.put(key, oldValue, vv, new int[] { columnIndex }, null);

        oldValue = map2.getTransactionalValue(key);
        columnIndex = 3;
        vv = createVersionedValue(map2, key, columnIndex, 1);
        map2.put(key, oldValue, vv, new int[] { columnIndex }, null);

        t2.rollback();
        // t2.commit();
        t1.commit();
        te.close();
    }

    public VersionedValue createVersionedValue(TransactionMap<String, VersionedValue> map, String key, int columnIndex,
            int value) {
        VersionedValue vv = map.get(key);
        Value[] values = vv.value.getList().clone();
        values[columnIndex] = ValueInt.get(value);
        ValueArray valueArray = ValueArray.get(values);
        vv = new VersionedValue(1, valueArray);
        return vv;
    }

    // @Test
    public void testAsyncTransaction() {
        PageOperationHandlerImpl.setPageOperationHandlersCount(3);
        PageOperationHandlerImpl.startPageOperationHandlers(null);
        TransactionEngine te = AMTransactionEngineTest.getTransactionEngine(false);
        Storage storage = AMTransactionEngineTest.getStorage();

        Transaction t = te.beginTransaction(false, false);
        TransactionMap<String, String> map = t.openMap("AsyncTransactionTest", storage);
        map.clear();

        MyTransactionListener listener = new MyTransactionListener();
        Object oldValue = map.get("1");
        map.put("1", oldValue, "a", null, listener);
        oldValue = map.get("2");
        map.put("2", oldValue, "b", null, listener);

        assertEquals("a", map.get("1"));
        assertEquals("b", map.get("2"));
        assertEquals(2, map.size());
        t.commit();

        t = te.beginTransaction(false, false);
        map = map.getInstance(t);
        oldValue = map.getTransactionalValue("1");
        map.put("1", oldValue, "a1", null, listener);
        assertEquals("a1", map.get("1"));
        assertEquals(2, map.size());
        t.commit();

        t.rollback();
        assertEquals(0, map.size());
        try {
            map.put("1", "a"); // 事务rollback或commit后就自动关闭了，java.lang.IllegalStateException: Transaction is closed
            fail();
        } catch (IllegalStateException e) {
        }

        t = te.beginTransaction(false, false);
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

        Transaction t2 = te.beginTransaction(false, false);
        map = map.getInstance(t2);
        map.put("3", "c");
        map.put("4", "d");
        assertEquals(4, map.size());

        Transaction t3 = te.beginTransaction(false, false);
        map = map.getInstance(t3);
        map.put("5", "f");
        assertEquals(3, map.size()); // t2未提交，所以读不到它put的数据

        Transaction t4 = te.beginTransaction(false, false);
        map = map.getInstance(t4);
        map.remove("1");
        assertEquals(1, map.size());
        t4.commit();

        Transaction t5 = te.beginTransaction(false, false);
        map = map.getInstance(t5);
        map.put("6", "g");
        assertEquals(2, map.size());
        t5.prepareCommit(); // 提交到后台，由LogSyncService线程在sync完事务日志后自动提交事务
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        assertEquals(Transaction.STATUS_CLOSED, t5.getStatus());

        te.close();
    }
}
