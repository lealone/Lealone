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
package org.lealone.test.aose;

import java.util.Random;

import org.junit.Test;
import org.lealone.storage.StorageMap;
import org.lealone.storage.aose.AOMap;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.AOStorageBuilder;
import org.lealone.test.TestBase;

//以单元测试的方式运行会比通过main方法运行得出稍微慢一些的测试结果，
//这可能是因为单元测试额外启动了一个ReaderThread占用了一些资源
public class AOMapPerformanceTest extends TestBase {

    public static void main(String[] args) throws Exception {
        new AOMapPerformanceTest().run();
    }

    private AOStorage storage;
    private String storageName;
    private StorageMap<Integer, String> map;

    @Test
    public void run() {
        // AOStorageService.getInstance().start();
        init();
        singleThread();
        multiThreads();
    }

    private void init() {
        AOStorageBuilder builder = new AOStorageBuilder();
        storageName = joinDirs("aose");
        int pageSplitSize = 16 * 1024;
        pageSplitSize = 2 * 1024;
        // pageSplitSize = 1 * 1024;
        // pageSplitSize = 32 * 1024;
        builder.storageName(storageName).compress().reuseSpace().pageSplitSize(pageSplitSize).minFillRate(30);
        storage = builder.openStorage();
        openMap();
    }

    private void openMap() {
        if (map == null || map.isClosed()) {
            // map = storage.openBTreeMap("AOMapPerformanceTest", null, null, null);
            map = storage.openAOMap("AOMapPerformanceTest", null, null, null);
        }
    }

    void singleThread() {
        int count = 50000;
        long t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.put(i, "valueaaa");
        }
        long t2 = System.currentTimeMillis();
        System.out.println("write time: " + (t2 - t1) + " ms");
        t1 = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            map.get(i);
        }
        t2 = System.currentTimeMillis();
        System.out.println("read time: " + (t2 - t1) + " ms");

        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("BufferedMap");
        for (int j = 0; j < 5; j++) {
            t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                map.get(i);
            }
            t2 = System.currentTimeMillis();
            System.out.println("read time: " + (t2 - t1) + " ms");
        }

        if (map instanceof AOMap)
            ((AOMap<Integer, String>) map).switchToNoBufferedMap();
        System.out.println("NoBufferedMap");
        for (int j = 0; j < 5; j++) {
            t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                map.get(i);
            }
            t2 = System.currentTimeMillis();
            System.out.println("read time: " + (t2 - t1) + " ms");
        }

        for (int j = 0; j < 5; j++) {
            t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                map.get(random.nextInt(count));
            }
            t2 = System.currentTimeMillis();
            System.out.println("random read time: " + (t2 - t1) + " ms");
        }
        System.out.println("size: " + map.size());

        if (map instanceof AOMap)
            ((AOMap<Integer, String>) map).switchToBufferedMap();
    }

    static Random random = new Random();

    class MyThread extends Thread {
        long read_time;
        long random_read_time;
        long write_time;
        int start;
        int end;

        MyThread(int start, int count) {
            super("MyThread-" + start);
            this.start = start;
            this.end = start + count;
        }

        void write() throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                map.put(i, "valueaaa");
            }

            long t2 = System.currentTimeMillis();
            write_time = t2 - t1;
            System.out.println(getName() + " write end, time=" + write_time + " ms");
        }

        void read(boolean random) throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                if (random)
                    map.get(AOMapPerformanceTest.random.nextInt(end));
                else
                    map.get(i);
            }
            long t2 = System.currentTimeMillis();

            if (random)
                random_read_time = t2 - t1;
            else
                read_time = t2 - t1;
            if (random)
                System.out.println(getName() + " random read end, time=" + random_read_time + " ms");
            else
                System.out.println(getName() + "  read end, time=" + read_time + " ms");
        }

        @Override
        public void run() {
            try {
                write();
                read(false);
                read(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    void multiThreads() {
        int threadsCount = 4; // Runtime.getRuntime().availableProcessors() * 4;
        int loop = 50000;
        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new MyThread(i * loop, loop);
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

        long write_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            write_sum += threads[i].write_time;
        }

        long read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            read_sum += threads[i].read_time;
        }
        long random_read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            random_read_sum += threads[i].random_read_time;
        }

        System.out.println();
        System.out.println("threads: " + threadsCount + ", loop: " + loop + ", rows: " + (threadsCount * loop));
        System.out.println("==========================================================");
        System.out.println("write_sum=" + write_sum + ", avg=" + (write_sum / threadsCount) + " ms");
        System.out.println("read_sum=" + read_sum + ", avg=" + (read_sum / threadsCount) + " ms");
        System.out.println("random_read_sum=" + random_read_sum + ", avg=" + (random_read_sum / threadsCount) + " ms");
    }
}
