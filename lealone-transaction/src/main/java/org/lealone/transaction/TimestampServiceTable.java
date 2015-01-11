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
package org.lealone.transaction;

import java.util.concurrent.atomic.AtomicLong;

import org.lealone.engine.Constants;
import org.lealone.mvstore.MVMap;
import org.lealone.mvstore.MVStore;

public class TimestampServiceTable {

    private TimestampServiceTable() {
    }

    private static final long TIMESTAMP_BATCH = Long.valueOf(System.getProperty(Constants.PROJECT_NAME_PREFIX
            + "transaction.timestamp.batch", "100000"));

    private static final String KEY = "k".intern();

    private static long first;
    private static final AtomicLong last = new AtomicLong();
    private static long maxTimestamp;

    private static MVMap<String, Long> map;

    public static synchronized void init(MVStore store) {
        if (map != null)
            return;

        map = store.openMap("timestampServiceTable", new MVMap.Builder<String, Long>());

        first = maxTimestamp = getLastMaxTimestamp();
        last.set(first);
        addBatch();
    }

    private static void updateLastMaxTimestamp(long lastMaxTimestamp) {
        map.put(KEY, lastMaxTimestamp);
    }

    private static long getLastMaxTimestamp() {
        Long lastMaxTimestamp = map.get(KEY);
        if (lastMaxTimestamp == null)
            return 0;
        return lastMaxTimestamp.longValue();
    }

    private static void addBatch() {
        maxTimestamp += TIMESTAMP_BATCH;
        updateLastMaxTimestamp(maxTimestamp);
    }

    public synchronized static void reset() {
        first = maxTimestamp = 0;
        last.set(first);
        updateLastMaxTimestamp(0);
        addBatch();
    }

    //事务用奇数版本号
    public static long nextOdd() {
        if (last.get() >= maxTimestamp) {
            synchronized (TimestampServiceTable.class) {
                addBatch();
            }
        }

        long oldLast;
        long last;
        long delta;
        do {
            oldLast = TimestampServiceTable.last.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 1;
            else
                delta = 2;

            last += delta;
        } while (!TimestampServiceTable.last.compareAndSet(oldLast, last));
        return last;
    }

    //非事务用偶数版本号
    public static long nextEven() {
        if (last.get() >= maxTimestamp) {
            synchronized (TimestampServiceTable.class) {
                addBatch();
            }
        }

        long oldLast;
        long last;
        long delta;
        do {
            oldLast = TimestampServiceTable.last.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 2;
            else
                delta = 1;
            last += delta;
        } while (!TimestampServiceTable.last.compareAndSet(oldLast, last));
        return last;
    }

    public static long first() {
        return first;
    }

    public static String toS() {
        return "TimestampServiceTable(first: " + first + ", last: " + last + ", max: " + maxTimestamp + ")";
    }
}
