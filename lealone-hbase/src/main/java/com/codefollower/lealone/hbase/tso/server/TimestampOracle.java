package com.codefollower.lealone.hbase.tso.server;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    public long next() throws IOException {
        last++;
        if (last == maxTimestamp) {
            maxTimestamp += TIMESTAMP_BATCH;
            // TODO 持久化最大值
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
