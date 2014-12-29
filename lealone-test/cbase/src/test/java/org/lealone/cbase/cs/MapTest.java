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
package org.lealone.cbase.cs;

import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MapTest {
    public static void main(String[] args) throws Exception {
        benchmark();
    }

    static AtomicInteger index = new AtomicInteger();

    private static ConcurrentNavigableMap<Integer, String> rows = new ConcurrentSkipListMap<Integer, String>();
    private static ConcurrentNavigableMap<Integer, String> rows2 = new ConcurrentSkipListMap<Integer, String>();
    private static ConcurrentNavigableMap<Integer, String> rows3 = new ConcurrentSkipListMap<Integer, String>();
    private static ConcurrentNavigableMap<Integer, String> rows4 = new ConcurrentSkipListMap<Integer, String>();

    //    private static final ConcurrentHashMap<Integer, String> rows = new ConcurrentHashMap<Integer, String>();
    //    private static final ConcurrentHashMap<Integer, String> rows2 = new ConcurrentHashMap<Integer, String>();
    //    private static final ConcurrentHashMap<Integer, String> rows3 = new ConcurrentHashMap<Integer, String>();
    //    private static final ConcurrentHashMap<Integer, String> rows4 = new ConcurrentHashMap<Integer, String>();
    //    
    //    private static final Map<Integer, String> rows = Collections.synchronizedMap(new HashMap<Integer, String>());
    //    private static final Map<Integer, String> rows2 = Collections.synchronizedMap(new HashMap<Integer, String>());
    //    private static final Map<Integer, String> rows3 = Collections.synchronizedMap(new HashMap<Integer, String>());
    //    private static final Map<Integer, String> rows4 = Collections.synchronizedMap(new HashMap<Integer, String>());

    static Random random = new Random();
    static CountDownLatch latch;

    static class MyThread extends Thread {
        long read_time;
        long randow_read_time;
        long write_time;
        int start;
        int end;

        MyThread(int start, int count) throws Exception {
            super("MyThread-" + start);
            this.start = start;
            this.end = start + count;
        }

        void write() throws Exception {

            long t1 = System.currentTimeMillis();
            String sql = "abc";
            for (int j = start; j < end; j++) {
                int i = index.getAndIncrement();
                //int hc = i % 4;
                rows.put(i, sql);

                //                if (hc == 0)
                //                    rows.put(i, sql);
                //                else if (hc == 1)
                //                    rows2.put(i, sql);
                //                else if (hc == 2)
                //                    rows3.put(i, sql);
                //                else
                //                    rows4.put(i, sql);
            }
            long t2 = System.currentTimeMillis();
            write_time = t2 - t1;
            System.out.println(getName() + " write end, time=" + write_time + " ms");
        }

        void read(boolean randow) throws Exception {
            long t1 = System.currentTimeMillis();
            for (int i = start; i < end; i++) {
                if (!randow)
                    rows.get(i);
                else
                    rows.get(random.nextInt(end));
            }

            long t2 = System.currentTimeMillis();

            if (randow)
                randow_read_time = t2 - t1;
            else
                read_time = t2 - t1;
            if (randow)
                System.out.println(getName() + " randow read end, time=" + randow_read_time + " ms");
            else
                System.out.println(getName() + "  read end, time=" + read_time + " ms");
        }

        @Override
        public void run() {
            try {
                write();
                read(false);
                read(true);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    static void benchmark() throws Exception {
        //预热阶段
        String str = "abc";
        for (int i = 0; i < 20000; i++) {
            rows.put(i, str);
            rows2.put(i, str);
            rows3.put(i, str);
            rows4.put(i, str);
        }
        rows.clear();
        rows2.clear();
        rows3.clear();
        rows4.clear();
        rows = new ConcurrentSkipListMap<Integer, String>();
        rows2 = new ConcurrentSkipListMap<Integer, String>();
        rows3 = new ConcurrentSkipListMap<Integer, String>();
        rows4 = new ConcurrentSkipListMap<Integer, String>();

        int threadsCount = 4;//Runtime.getRuntime().availableProcessors();//10;
        int loop = 1500000;
        latch = new CountDownLatch(threadsCount);

        MyThread[] threads = new MyThread[threadsCount];
        for (int i = 0; i < threadsCount; i++) {
            threads[i] = new MyThread(i * loop, loop);
        }

        for (int i = 0; i < threadsCount; i++) {
            threads[i].start();
        }

        latch.await();

        long write_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            write_sum += threads[i].write_time;
        }

        long read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            read_sum += threads[i].read_time;
        }
        long randow_read_sum = 0;
        for (int i = 0; i < threadsCount; i++) {
            randow_read_sum += threads[i].randow_read_time;
        }

        System.out.println();
        System.out.println("threads: " + threadsCount + ", loop: " + loop + ", rows: " + (threadsCount * loop));
        System.out.println("==========================================================");
        System.out.println("write_sum=" + write_sum + ", avg=" + (write_sum / threadsCount) + " ms");
        System.out.println("read_sum=" + read_sum + ", avg=" + (read_sum / threadsCount) + " ms");
        System.out.println("randow_read_sum=" + randow_read_sum + ", avg=" + (randow_read_sum / threadsCount) + " ms");

        System.out.println("r1.size=" + rows.size());
        System.out.println("r2.size=" + rows2.size());
        System.out.println("r3.size=" + rows3.size());
        System.out.println("r4.size=" + rows4.size());
    }

}
