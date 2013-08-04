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

import com.codefollower.lealone.hbase.engine.HBaseConstants;
import com.codefollower.lealone.hbase.metadata.TimestampServiceTable;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;

public class TimestampService {
    private static final long TIMESTAMP_BATCH = HBaseUtils.getConfiguration().getLong(HBaseConstants.TRANSACTION_TIMESTAMP_BATCH,
            HBaseConstants.DEFAULT_TRANSACTION_TIMESTAMP_BATCH);

    private final TimestampServiceTable timestampServiceTable;
    private long first;
    private long last;
    private long maxTimestamp;

    public TimestampService(String hostAndPort) {
        try {
            timestampServiceTable = new TimestampServiceTable(hostAndPort);
            first = last = maxTimestamp = timestampServiceTable.getLastMaxTimestamp();
            addBatch();
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    public long first() {
        return first;
    }

    private void addBatch() throws IOException {
        maxTimestamp += TIMESTAMP_BATCH;
        timestampServiceTable.updateLastMaxTimestamp(maxTimestamp);
    }

    public synchronized void reset() throws IOException {
        first = last = maxTimestamp = 0;
        timestampServiceTable.updateLastMaxTimestamp(0);
        addBatch();
    }

    //事务用奇数版本号
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

    //非事务用偶数版本号
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

    @Override
    public String toString() {
        return "TimestampService(first: " + first + ", last: " + last + ", max: " + maxTimestamp + ")";
    }
}
