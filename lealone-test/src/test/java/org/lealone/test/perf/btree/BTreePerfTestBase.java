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
package org.lealone.test.perf.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;

import org.lealone.storage.StorageMap;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.test.TestBase;

//以单元测试的方式运行会比通过main方法运行得出稍微慢一些的测试结果，
//这可能是因为单元测试额外启动了一个ReaderThread占用了一些资源
public class BTreePerfTestBase extends TestBase {

    protected AOStorage storage;
    protected String storagePath;
    protected StorageMap<Integer, String> map;

    static int threadsCount = 3; // Runtime.getRuntime().availableProcessors() * 4;
    static int count = 90000 * 1;// 50000;

    int[] randomKeys = getRandomKeys();

    // @Test
    public void run() {
        init();
        singleThreadSerialWrite();
        // singleThreadRandomWrite();

        int loop = 20;
        for (int i = 1; i <= loop; i++) {
            // map.clear();

            singleThreadRandomWrite();
            // singleThreadSerialWrite();

            // singleThreadRandomRead();
            // singleThreadSerialRead();

            // multiThreadsRandomWrite(i);
            // multiThreadsSerialWrite(i);

            // multiThreadsRandomRead(i);
            // multiThreadsSerialRead(i);
        }

        // testSystemArraycopy();

        // for (int i = 0; i < 40; i++) {
        // testConcurrentLinkedQueue();
        // testLinkedTransferQueue();
        // }

        // for (int i = 0; i < 40; i++) {
        // // testCurrentThread();
        // testCountDownLatch();
        // }

        // testConcurrentSkipListMap();
    }

    protected void init() {
        // PageOperationHandler.startPageOperationHandlers(null);
        AOStorageBuilder builder = new AOStorageBuilder();
        storagePath = joinDirs("aose");
        int pageSplitSize = 16 * 1024;
        // pageSplitSize = 2 * 1024;
        // pageSplitSize = 4 * 1024;
        pageSplitSize = 1 * 1024;
        // pageSplitSize = 1024 / 2 / 2;
        // pageSplitSize = 32 * 1024;
        // pageSplitSize = 512 * 1024;
        builder.storagePath(storagePath).compress().reuseSpace().pageSplitSize(pageSplitSize).minFillRate(30);
        storage = builder.openStorage();
        openMap();
    }

    protected void openMap() {
    }

    void testSystemArraycopy() {
        Object[] src = new Object[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        int len = src.length;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Object[] dest = new Object[len];
            System.arraycopy(src, 0, dest, 0, len);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("SystemArraycopy time: " + (t2 - t1) + " ms, count: " + count);
    }

    void testConcurrentLinkedQueue() {
        int count = 50000;
        long t1 = System.currentTimeMillis();
        ConcurrentLinkedQueue<String> tasks = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < count; i++) {
            tasks.add("abc");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("ConcurrentLinkedQueue add time: " + (t2 - t1) + " ms, count: " + count);
    }

    void testLinkedTransferQueue() {
        int count = 50000;
        long t1 = System.currentTimeMillis();
        LinkedTransferQueue<String> tasks = new LinkedTransferQueue<>();
        for (int i = 0; i < count; i++) {
            tasks.add("abc");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("LinkedTransferQueue add time: " + (t2 - t1) + " ms, count: " + count);
    }

    void testCurrentThread() {
        int count = 50000;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Thread.currentThread();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("invoke Thread.currentThread time: " + (t2 - t1) + " ms, count: " + count);
    }

    void testCountDownLatch() {
        int count = 50000;
        CountDownLatch latch = new CountDownLatch(count);
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            latch.countDown();
        }
        long t2 = System.currentTimeMillis();
        System.out.println("CountDownLatch time: " + (t2 - t1) + " ms, count: " + count);
    }

    private final ConcurrentSkipListMap<Integer, String> skipListMap = new ConcurrentSkipListMap<>();

    void testConcurrentSkipListMap() {
        for (int loop = 0; loop < 20; loop++) {
            // skipListMap.clear();
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                skipListMap.put(i, "valueaaa");
            }
            long t2 = System.currentTimeMillis();
            System.out.println("ConcurrentSkipListMap serial write time: " + (t2 - t1) + " ms, count: " + count);
        }
        System.out.println();
        int[] keys = getRandomKeys();
        for (int loop = 0; loop < 20; loop++) {
            // skipListMap.clear(); //不clear时更快一些
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                skipListMap.put(keys[i], "valueaaa");
            }
            long t2 = System.currentTimeMillis();
            System.out.println("ConcurrentSkipListMap random write time: " + (t2 - t1) + " ms, count: " + count);
        }
    }

    int[] getRandomKeys() {
        ArrayList<Integer> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(i);
        }
        Collections.shuffle(list);
        int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = list.get(i);
        }
        return keys;
    }

    void singleThreadSerialWrite() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.put(i, "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial write time: " + (t2 - t1) + " ms, count: " + count);
    }

    void singleThreadRandomWrite() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.put(randomKeys[i], "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random write time: " + (t2 - t1) + " ms, count: " + map.size());
    }

    void singleThreadSerialRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.get(i);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread serial read time: " + (t2 - t1) + " ms, count: " + count);
    }

    void singleThreadRandomRead() {
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.get(randomKeys[i]);
        }
        long t2 = System.currentTimeMillis();
        System.out.println("single-thread random read time: " + (t2 - t1) + " ms, count: " + count);
    }

    class MyThread extends Thread {
        int start;
        int end;
        boolean read;
        boolean random;

        long readTime;
        long writeTime;
        String timeStr;

        MyThread(int start, int end, boolean read, boolean random) {
            super("MyThread-" + start);
            this.start = start;
            this.end = end;
            this.read = read;
            this.random = random;
        }

        void write() throws Exception {
            for (int i = start; i < end; i++) {
                int key;
                if (random)
                    key = randomKeys[i];
                else
                    key = i;
                String value = "value-";// "value-" + key;
                map.put(key, value);
            }
        }

        void read() throws Exception {
            for (int i = start; i < end; i++) {
                if (random)
                    map.get(randomKeys[i]);
                else
                    map.get(i);
            }
        }

        @Override
        public void run() {
            try {
                long t1 = System.currentTimeMillis();
                if (read) {
                    read();
                } else {
                    write();
                }
                long t2 = System.currentTimeMillis();
                if (read) {
                    readTime = t2 - t1;
                } else {
                    writeTime = t2 - t1;
                }
                timeStr = (getName() + (random ? " random " : " serial ") + (read ? "read" : "write") + " end, time: "
                        + (t2 - t1) + " ms, count: " + (end - start));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    void multiThreadsSerialRead(int loop) {
        multiThreads(loop, true, false);
    }

    void multiThreadsRandomRead(int loop) {
        multiThreads(loop, true, true);
    }

    void multiThreadsSerialWrite(int loop) {
        multiThreads(loop, false, false);
    }

    void multiThreadsRandomWrite(int loop) {
        multiThreads(loop, false, true);
    }

    void multiThreads(int loop, boolean read, boolean random) {
        int avg = count / threadsCount;
        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            int start = i * avg;
            int end = (i + 1) * avg;
            if (i == threadsCount - 1)
                end = count;
            threads[i] = new MyThread(start, end, read, random);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadsCount; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        long timeSum = 0;
        if (read) {
            for (int i = 0; i < threadsCount; i++) {
                timeSum += threads[i].readTime;
            }
        } else {
            for (int i = 0; i < threadsCount; i++) {
                timeSum += threads[i].writeTime;
            }
        }
        System.out.println();
        System.out.println("loop: " + loop + ", threads: " + threadsCount + ", count: " + count);
        System.out.println("==========================================================");
        for (int i = 0; i < threadsCount; i++) {
            System.out.println(threads[i].timeStr);
        }
        System.out.println("multi-threads" + (random ? " random " : " serial ") + (read ? "read" : "write")
                + " time, sum: " + timeSum + " ms, avg: " + (timeSum / threadsCount) + " ms");
        System.out.println("==========================================================");
    }
}
