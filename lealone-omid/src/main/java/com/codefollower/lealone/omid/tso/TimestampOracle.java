/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.lealone.omid.tso;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.codefollower.lealone.omid.tso.persistence.LoggerProtocol;

/**
 * The Timestamp Oracle that gives monotonically increasing timestamps
 * 
 * @author maysam
 * 
 */

public class TimestampOracle {

    private static final Log LOG = LogFactory.getLog(TimestampOracle.class);

    private static final long TIMESTAMP_BATCH = 100000;

    private long maxTimestamp = 1;

    /**
     * the last returned timestamp
     */
    private long last;
    private long first;

    /**
     * Constructor
     */
    public TimestampOracle() {
        last = 0;
    }

    /**
     * Starts with a given timestamp.
     * 
     * @param timestamp
     */
    public void initialize(long timestamp) {
        LOG.info("Initializing timestamp oracle");

        last = first = Math.max(last, timestamp + TIMESTAMP_BATCH);
        maxTimestamp = first + 1; // max timestamp will be persisted

        LOG.info("First: " + first + ", Last: " + last);
    }

    /**
     * Must be called holding an exclusive lock
     * 
     * return the next timestamp
     */
    public long next(DataOutputStream toWal) throws IOException {
        last++;
        if (last == maxTimestamp) {
            maxTimestamp += TIMESTAMP_BATCH;
            toWal.writeByte(LoggerProtocol.TIMESTAMP_ORACLE);
            toWal.writeLong(maxTimestamp);
        }

        return last;
    }

    public long get() {
        return last;
    }

    public long first() {
        return first;
    }

    @Override
    public String toString() {
        return "TimestampOracle(first: " + first + ", last: " + last + ")";
    }
}
