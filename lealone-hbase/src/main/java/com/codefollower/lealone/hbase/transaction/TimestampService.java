/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.transaction;

import java.io.IOException;

public class TimestampService {
    private static final long TIMESTAMP_BATCH = 100000;

    private long first = 0;
    private long last = 0;
    private long maxTimestamp = 0;

    public void initialize(long lastMaxTimestamp) {
        first = last = maxTimestamp = lastMaxTimestamp;

        addBatch();
    }

    private void addBatch() {
        maxTimestamp += TIMESTAMP_BATCH;
        // TODO 持久化最大值
    }

    public synchronized long nextOdd() throws IOException {
        if (last >= maxTimestamp)
            addBatch();

        long delta;
        if (last % 2 == 0)
            delta = 1;
        else
            delta = 2;

        last += delta;
        return last;
    }

    public synchronized long nextEven() throws IOException {
        if (last >= maxTimestamp)
            addBatch();

        long delta;
        if (last % 2 == 0)
            delta = 2;
        else
            delta = 1;
        last += delta;
        return last;
    }

    public long first() {
        return first;
    }

    public long last() {
        return first;
    }

    @Override
    public String toString() {
        return "TimestampService(first: " + first + ", last: " + last + ", max: " + maxTimestamp + ")";
    }
}
