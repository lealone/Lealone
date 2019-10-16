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
package org.lealone.test.perf.transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.storage.DefaultPageOperationHandler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.StorageMap;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.test.TestBase;
import org.lealone.test.amte.AMTransactionEngineTest;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionMap;

//以单元测试的方式运行会比通过main方法运行得出稍微慢一些的测试结果，
//这可能是因为单元测试额外启动了一个ReaderThread占用了一些资源
public class TransactionPerfTestBase {

    protected AOStorage storage;
    protected String storagePath;
    protected StorageMap<Integer, String> map;

    static int threadCount = 4; // Runtime.getRuntime().availableProcessors();
    static int rowCount = 90000;// 10000 * 10 * 10 * 1; // 总记录数
    static int loopCount = 10; // 重复测试次数
    static int[] randomKeys = getRandomKeys();

    static int conflictKeyCount = 10000 * 5; // 冲突key个数
    static int[] conflictKeys = getConflictKeys();

    CountDownLatch latch;
    final AtomicLong startTime = new AtomicLong(0);
    final AtomicLong endTime = new AtomicLong(0);
    final AtomicLong pendingPageOperations = new AtomicLong(0);
    final HashMap<String, String> config = new HashMap<>();
    boolean testConflictOnly;

    private TransactionEngine te;
    String mapName;

    protected void testWrite(int loop) {
        // singleThreadRandomWrite();
        // singleThreadSerialWrite();
        multiThreadsRandomWrite(loop);
        multiThreadsSerialWrite(loop);
    }

    protected void testRead(int loop) {
        // singleThreadRandomRead();
        // singleThreadSerialRead();

        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);
    }

    protected void testConflict(int loop) {
        testConflict(loop, false);
    }

    // @Test
    public void run() {
        // for (int j = 0; j < 20; j++) {
        // long t1 = System.currentTimeMillis();
        // for (int i = 0; i < 25000; i++) {
        // ByteBuffer.allocate(1024 * 1024);
        // // DataBuffer writeBuffer = DataBuffer.create(1024 * 1024);
        // // try (DataBuffer writeBuffer = DataBuffer.create()) {
        // // ByteBuffer buffer = writeBuffer.getAndFlipBuffer();
        // // ByteBuffer operations = ByteBuffer.allocateDirect(buffer.limit());
        // // operations.put(buffer);
        // // operations.flip();
        // // }
        // }
        // long t2 = System.currentTimeMillis();
        // System.out.println("new DataBuffer time: " + (t2 - t1) + " ms");
        // }

        init();
        loopCount = 5;

        int availableProcessors = Runtime.getRuntime().availableProcessors();
        threadCount = availableProcessors;
        run0();

        // threadCount = availableProcessors * 2;
        // run0();
        //
        // threadCount = availableProcessors * 4;
        // run0();
        //
        // threadCount = 100;
        // run0();
        //
        // // 同样是完成500万次更新操作，
        // // 对于高并发高冲突的场景，只开availableProcessors个线程多循环几次效果更好
        // threadCount = availableProcessors;
        // loopCount = 100 / threadCount;
        // testConflictOnly = true;
        // run0();
    }

    private void run0() {
        beforeRun();
        loop();
    }

    protected void beforeRun() {
        // singleThreadSerialWrite();// 先生成初始数据
        // System.out.println("map size: " + map.size());

        // singleThreadRandomWrite();
        // multiThreadsRandomRead(0);
        // multiThreadsSerialWriteAsync(0);

        // singleThreadSerialWrite0();
    }

    void singleThreadSerialWrite0() {
        Transaction t = te.beginTransaction(false);
        TransactionMap<Integer, String> map = t.openMap(mapName, storage);
        this.map = map;
        map.clear();
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(i, "valueaaa");
        }
        t.commit();
        long t2 = System.currentTimeMillis();

        System.out.println("single-thread serial write time: " + (t2 - t1) + " ms, count: " + map.size());
    }

    private void loop() {
        long t1 = System.currentTimeMillis();
        for (int i = 1; i <= loopCount; i++) {
            // map.clear();
            if (!testConflictOnly) {
                testWrite(i);
                // testRead(i);
            }
            // testConflict(i);

            System.out.println();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("total time: " + (t2 - t1) + " ms");
        System.out.println("map size: " + map.size());
        System.out.println();
    }

    protected void init() {
        String factoryType = "RoundRobin";
        factoryType = "Random";
        // factoryType = "LoadBalance";
        config.put("page_operation_handler_factory_type", factoryType);
        config.put("page_operation_handler_count", (threadCount + 1) + "");
        AOStorageBuilder builder = new AOStorageBuilder(config);
        storagePath = TestBase.joinDirs("aose");
        int pageSplitSize = 16 * 1024;
        // pageSplitSize = 2 * 1024;
        // pageSplitSize = 4 * 1024;
        // pageSplitSize = 1 * 1024;
        // pageSplitSize = 1024 / 2 / 2;
        // pageSplitSize = 32 * 1024;
        // pageSplitSize = 512 * 1024;
        builder.storagePath(storagePath).compress().reuseSpace().pageSplitSize(pageSplitSize).minFillRate(30);
        storage = builder.openStorage();
        te = AMTransactionEngineTest.getTransactionEngine();
        openMap();
    }

    protected void openMap() {
    }

    static int[] getRandomKeys() {
        ArrayList<Integer> list = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        int[] keys = new int[rowCount];
        for (int i = 0; i < rowCount; i++) {
            keys[i] = list.get(i);
        }
        return keys;
    }

    static int[] getConflictKeys() {
        Random random = new Random();
        int[] keys = new int[conflictKeyCount];
        for (int i = 0; i < conflictKeyCount; i++) {
            keys[i] = random.nextInt(rowCount);
        }
        return keys;
    }

    void singleThreadSerialWrite() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(i, "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial write time: " + (t2 - t1) + " ms, count: " + rowCount);
    }

    void singleThreadRandomWrite() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.put(randomKeys[i], "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random write time: " + (t2 - t1) + " ms, count: " + map.size());
    }

    void singleThreadSerialRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.get(i);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial read time: " + (t2 - t1) + " ms, count: " + rowCount);
    }

    void singleThreadRandomRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            map.get(randomKeys[i]);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random read time: " + (t2 - t1) + " ms, count: " + rowCount);
    }

    protected Thread getThread(PageOperation pageOperation, int index, int start) {
        return new Thread(pageOperation, "MyThread-" + start);
    }

    protected DefaultPageOperationHandler getDefaultPageOperationHandler(int index) {
        return null;
    }

    private void notifyOperationComplete() {
        if (pendingPageOperations.decrementAndGet() <= 0) {
            endTime.set(System.currentTimeMillis());
            latch.countDown();
        }
    }

    abstract class PerfTestThread implements Runnable, PageOperation {
        final Thread thread;
        boolean async;
        long shiftCount;

        PerfTestThread(int threadIndex, int start, boolean async) {
            thread = getThread(this, threadIndex, start);
            this.async = async;
        }

        @Override
        public void run() {
            run(null);
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            // 取最早启动的那个线程的时间
            startTime.compareAndSet(0, System.currentTimeMillis());
            try {
                runInternal(currentHandler);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return PageOperationResult.SUCCEEDED;
        }

        abstract void runInternal(PageOperationHandler currentHandler) throws Exception;

        void start() {
            thread.start();
        }

        void join() throws InterruptedException {
            thread.join();
        }

        void interrupt() {
            thread.interrupt();
        }
    }

    class PageOperationPerfTestThread extends PerfTestThread {
        int start;
        int end;
        boolean read;
        boolean random;

        PageOperationPerfTestThread(int threadIndex, int start, int end, boolean read, boolean random, boolean async) {
            super(threadIndex, start, async);
            this.start = start;
            this.end = end;
            this.read = read;
            this.random = random;
        }

        @Override
        void runInternal(PageOperationHandler currentHandler) throws Exception {
            if (read) {
                read();
            } else {
                write(currentHandler);
            }
        }

        void read() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (random)
                    key = randomKeys[i];
                else
                    key = i;
                if (async) {
                    map.get(key, ar -> {
                        notifyOperationComplete();
                    });
                } else {
                    map.get(i);
                    notifyOperationComplete();
                }
            }
        }

        void write(PageOperationHandler currentHandler) throws Exception {
            Transaction t = te.beginTransaction(false);
            TransactionMap<Integer, String> map = t.openMap(mapName, storage);
            for (int i = start; i < end; i++) {
                Integer key;
                if (random)
                    key = randomKeys[i];
                else
                    key = i;
                String value = "value-";// "value-" + key;
                // map.put(key, value);

                Transaction t2 = te.beginTransaction(false);
                TransactionMap<Integer, String> m = map.getInstance(t2);
                m.tryUpdate(key, value);
                t2.commit();
                // System.out.println(getName() + " key:" + key);
            }
            endTime.set(System.currentTimeMillis());
            latch.countDown();
        }
    }

    class ConflictPerfTestThread extends PerfTestThread {
        ConflictPerfTestThread(int threadIndex, boolean async) {
            super(threadIndex, threadIndex, async);
        }

        @Override
        void runInternal(PageOperationHandler currentHandler) throws Exception {
            for (int i = 0; i < conflictKeyCount; i++) {
                int key = conflictKeys[i];
                String value = "value-conflict";

                if (async) {
                    PageOperation po = map.createPutOperation(key, value, ar -> {
                        notifyOperationComplete();
                    });
                    PageOperationResult result = po.run(currentHandler);
                    if (result == PageOperationResult.SHIFTED) {
                        shiftCount++;
                    }
                } else {
                    map.put(key, value);
                    notifyOperationComplete();
                }
            }
        }
    }

    void multiThreadsSerialRead(int loop) {
        multiThreads(loop, true, false, false);
    }

    void multiThreadsRandomRead(int loop) {
        multiThreads(loop, true, true, false);
    }

    void multiThreadsSerialWrite(int loop) {
        multiThreads(loop, false, false, false);
    }

    void multiThreadsRandomWrite(int loop) {
        multiThreads(loop, false, true, false);
    }

    void multiThreadsSerialReadAsync(int loop) {
        multiThreads(loop, true, false, true);
    }

    void multiThreadsRandomReadAsync(int loop) {
        multiThreads(loop, true, true, true);
    }

    void multiThreadsSerialWriteAsync(int loop) {
        multiThreads(loop, false, false, true);
    }

    void multiThreadsRandomWriteAsync(int loop) {
        multiThreads(loop, false, true, true);
    }

    void multiThreads(int loop, boolean read, boolean random, boolean async) {
        int avg = rowCount / threadCount;
        PageOperationPerfTestThread[] threads = new PageOperationPerfTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (i == threadCount - 1)
                end = rowCount;
            threads[i] = new PageOperationPerfTestThread(i, start, end, read, random, async);
        }
        startTest(threads, rowCount);
        // String shiftStr = getShiftStr(threads, async);

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadCount;

        // System.out.println(map.getName() + " loop: " + loop + ", rows: " + rowCount + ", threads: " + threadCount
        // + shiftStr + ", " + (async ? "async " : "sync ") + (random ? "random " : "serial ")
        // + (read ? "read " : "write") + ", total time: " + totalTime + " ms, avg time: " + avgTime + " ms");
        //
        System.out.println(mapName + " loop: " + loop + ", rows: " + rowCount + ", threads: " + threadCount
                + (random ? ", random " : ", serial ") + (read ? "read " : "write") + ", total time: " + totalTime
                + " ms, avg time: " + avgTime + " ms");
    }

    // 异步场景下线程移交PageOperation的次数
    private String getShiftStr(PerfTestThread[] threads, boolean async) {
        String shiftStr = "";
        if (async) {
            long shiftSum = 0;
            for (int i = 0; i < threadCount; i++) {
                shiftSum += threads[i].shiftCount;
                DefaultPageOperationHandler h = getDefaultPageOperationHandler(i);
                shiftSum += h.getShiftCount();
            }
            shiftStr = ", shift: " + shiftSum;
        }
        return shiftStr;
    }

    private void startTest(PerfTestThread[] threads, int keyCount) {
        latch = new CountDownLatch(threadCount);
        startTime.set(0);
        endTime.set(0);
        pendingPageOperations.set(keyCount);

        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        try {
            latch.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }

        for (int i = 0; i < threadCount; i++) {
            threads[i].interrupt();
        }
        for (int i = 0; i < threadCount; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void testConflict(int loop, boolean async) {
        ConflictPerfTestThread[] threads = new ConflictPerfTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new ConflictPerfTestThread(i, async);
        }
        startTest(threads, conflictKeyCount);
        String shiftStr = getShiftStr(threads, async);

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadCount;

        System.out.println(map.getName() + " loop: " + loop + ", rows: " + rowCount + ", threads: " + threadCount
                + ", conflict keys: " + conflictKeyCount + shiftStr + ", " + (async ? "async" : "sync")
                + " write conflict, total time: " + totalTime + " ms, avg time: " + avgTime + " ms");
    }
}
