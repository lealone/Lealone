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
package org.lealone.test.perf.sql;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.client.jdbc.JdbcStatement;
import org.lealone.server.Scheduler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.test.TestBase;

public class EmbeddedSqlPerfTestBase {

    static int threadCount = 5; // Runtime.getRuntime().availableProcessors();
    static int rowCount = 2000;// 10000 * 10 * 10 * 1; // 总记录数
    static int loopCount = 10; // 重复测试次数
    static int[] randomKeys = getRandomKeys();

    static int conflictKeyCount = 10000 * 5; // 冲突key个数
    static int[] conflictKeys = getConflictKeys();

    private CountDownLatch latch;
    private final AtomicLong startTime = new AtomicLong(0);
    private final AtomicLong endTime = new AtomicLong(0);
    private final AtomicLong pendingPageOperations = new AtomicLong(0);
    private boolean testConflictOnly;

    protected void testWrite(int loop) {
        multiThreadsRandomWrite(loop);
        multiThreadsSerialWrite(loop);
    }

    protected void testRead(int loop) {
        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);
    }

    protected void testConflict(int loop) {
        testConflict(loop, false);
    }

    public void run() {
        beforeRun();
        loop();
    }

    protected void beforeRun() {
    }

    private Connection getConnection() throws Exception {
        TestBase.initTransactionEngine();
        TestBase test = new TestBase();
        // test.setInMemory(true);
        test.setEmbedded(true);
        // test.printURL();
        return test.getConnection();
    }

    void singleThreadSerialWrite() {
        try {
            Connection conn = getConnection();
            init(conn, rowCount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init(Connection conn, int rowCount) throws Exception {
        Statement statement = conn.createStatement();
        statement.executeUpdate("drop table IF EXISTS SqlPerfTest");
        statement.executeUpdate("create table IF NOT EXISTS SqlPerfTest(f1 int primary key , f2 varchar(20))");
        // statement.executeUpdate("create index index0 on SqlPerfTest(f2)");
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < rowCount; i++) {
            String sql = "insert into SqlPerfTest values(" + i + ",'value-" + i + "')";
            statement.executeUpdate(sql);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("insert rowCount: " + rowCount + ", time: " + (t2 - t1) + " ms");
        statement.close();
    }

    private void loop() {
        long t1 = System.currentTimeMillis();
        for (int i = 1; i <= loopCount; i++) {
            if (!testConflictOnly) {
                testWrite(i);
                // testRead(i);
            }
            // testConflict(i);

            System.out.println();
        }
        for (int i = 0; i < threadCount; i++) {
            getScheduler(i).interrupt();
        }
        for (int i = 0; i < threadCount; i++) {
            try {
                getScheduler(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long t2 = System.currentTimeMillis();
        System.out.println("total time: " + (t2 - t1) + " ms");
        System.out.println("rowCount: " + rowCount);
        System.out.println();
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

    protected Thread getThread(PageOperation pageOperation, int index, int start) {
        return new Thread(pageOperation, "MyThread-" + start);
    }

    protected Scheduler getScheduler(int index) {
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
            // for (int i = start; i < end; i++) {
            // int key;
            // if (random)
            // key = randomKeys[i];
            // else
            // key = i;
            // if (async) {
            //
            // } else {
            // }
            // }
        }

        void write(PageOperationHandler currentHandler) throws Exception {
            // int rowCount2 = end - start;
            // CountDownLatch latch = new CountDownLatch(1);
            // AtomicLong count = new AtomicLong(rowCount2);
            Connection conn = getConnection();

            JdbcStatement statement = (JdbcStatement) conn.createStatement();
            for (int i = start; i < end; i++) {
                Integer key;
                if (random)
                    key = randomKeys[i];
                else
                    key = i;
                // String value = "value-";// "value-" + key;
                // map.put(key, value);

                String sql = "update SqlPerfTest set f2 = 'pet2' where f1 =" + key;
                statement.executeUpdate(sql);
                notifyOperationComplete();
                // System.out.println(getName() + " key:" + key);
                // AsyncHandler<AsyncResult<Integer>> handler = ar -> {
                // // if (count.decrementAndGet() <= 0) {
                // //
                // // endTime.set(System.currentTimeMillis());
                // // latch.countDown();
                // // }
                // notifyOperationComplete();
                // };
                // statement.executeUpdateAsync(sql, handler);
            }
            // latch.await();
            // statement.close();
        }
    }

    class ConflictPerfTestThread extends PerfTestThread {
        ConflictPerfTestThread(int threadIndex, boolean async) {
            super(threadIndex, threadIndex, async);
        }

        @Override
        void runInternal(PageOperationHandler currentHandler) throws Exception {
            for (int i = 0; i < conflictKeyCount; i++) {
                // int key = conflictKeys[i];
                // String value = "value-conflict";
                //
                // if (async) {
                // PageOperation po = map.createPutOperation(key, value, ar -> {
                // notifyOperationComplete();
                // });
                // PageOperationResult result = po.run(currentHandler);
                // if (result == PageOperationResult.SHIFTED) {
                // shiftCount++;
                // }
                // } else {
                // map.put(key, value);
                // notifyOperationComplete();
                // }
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

    void multiThreadsSerialWriteAsync(int loop) {
        multiThreads(loop, false, false, true);
    }

    void multiThreadsRandomWriteAsync(int loop) {
        multiThreads(loop, false, true, true);
    }

    void multiThreads(int loop, boolean read, boolean random, boolean async) {
        reset(rowCount);
        int avg = rowCount / threadCount;
        PageOperationPerfTestThread[] threads = new PageOperationPerfTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (i == threadCount - 1)
                end = rowCount;
            threads[i] = new PageOperationPerfTestThread(i, start, end, read, random, async);
        }
        await();

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadCount;

        System.out.println(" loop: " + loop + ", rows: " + rowCount + ", threads: " + threadCount
                + (random ? ", random " : ", serial ") + (read ? "read " : "write") + ", total time: " + totalTime
                + " ms, avg time: " + avgTime + " ms");
    }

    private void reset(int keyCount) {
        latch = new CountDownLatch(1);
        startTime.set(0);
        endTime.set(0);
        pendingPageOperations.set(keyCount);
    }

    private void await() {
        try {
            latch.await();
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
    }

    void testConflict(int loop, boolean async) {
        reset(rowCount);
        ConflictPerfTestThread[] threads = new ConflictPerfTestThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new ConflictPerfTestThread(i, async);
        }
        await();
        String shiftStr = "";

        long totalTime = endTime.get() - startTime.get();
        long avgTime = totalTime / threadCount;

        System.out.println(" loop: " + loop + ", rows: " + rowCount + ", threads: " + threadCount + ", conflict keys: "
                + conflictKeyCount + shiftStr + ", " + (async ? "async" : "sync") + " write conflict, total time: "
                + totalTime + " ms, avg time: " + avgTime + " ms");
    }
}
