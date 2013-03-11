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

package com.yahoo.omid.tso;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.yahoo.omid.tso.persistence.LoggerProtocol;

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

    @SuppressWarnings("unused")
    private boolean enabled;

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
            if (LOG.isTraceEnabled()) {
                LOG.trace("Logging TimestampOracle " + maxTimestamp);
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Next timestamp: " + last);
        }

        return last;
    }

    public long get() {
        return last;
    }

    public long first() {
        return first;
    }

    //private static final String BACKUP = "/tmp/tso-persist.backup";
    //private static final String PERSIST = "/tmp/tso-persist.txt";

    /**
     * Constructor
     */
    public TimestampOracle() {
        this.enabled = false;
        this.last = 0;
    }

    /**
     * Starts from scratch.
     */
    public void initialize() {
        this.enabled = true;
    }

    /**
     * Starts with a given timestamp.
     * 
     * @param timestamp
     */
    public void initialize(long timestamp) {
        LOG.info("Initializing timestamp oracle");
        this.last = this.first = Math.max(this.last, timestamp + TIMESTAMP_BATCH);
        maxTimestamp = this.first + 1; // max timestamp will be persisted
        LOG.info("First: " + this.first + ", Last: " + this.last);
        initialize();
    }

    public void stop() {
        this.enabled = false;
    }

    @Override
    public String toString() {
        return "TimestampOracle: " + last;
    }
}
