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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.codefollower.lealone.omid.replication.SharedMessageBuffer;
import com.codefollower.lealone.omid.tso.persistence.StateLogger;
import com.codefollower.lealone.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.codefollower.lealone.omid.tso.persistence.LoggerException.Code;

/**
 * The wrapper for different states of TSO
 * This state is shared between handlers
 * @author maysam
 */
public class TSOState {
    /**
     * Hash map load factor
     */
    public static final float LOAD_FACTOR = 0.5f;

    /**
     * The maximum entries kept in TSO
     */
    public static int MAX_ITEMS = 100;
    public static int MAX_COMMITS = 100;
    public static int FLUSH_TIMEOUT = 10;

    static {
        try {
            MAX_ITEMS = Integer.valueOf(System.getProperty("omid.maxItems"));
        } catch (Exception e) {
            // ignore, usedefault
        }

        try {
            MAX_COMMITS = Integer.valueOf(System.getProperty("omid.maxCommits"));
        } catch (Exception e) {
            // ignore, usedefault
        }

        try {
            FLUSH_TIMEOUT = Integer.valueOf(System.getProperty("omid.flushTimeout"));
        } catch (Exception e) {
            // ignore, usedefault
        }
    }

    /**
     * Object that implements the logic to log records
     * for recoverability
     */
    private StateLogger logger;

    /**
     * Only timestamp oracle instance in the system.
     */
    private TimestampOracle timestampOracle;

    /**
     * Largest Deleted Timestamp
     */
    public long largestDeletedTimestamp = 0;
    public long previousLargestDeletedTimestamp = 0;

    public SharedMessageBuffer sharedMessageBuffer = new SharedMessageBuffer();

    /**
     * The hash map to to keep track of recently committed rows
     * each bucket is about 20 byte, so the initial capacity is 20MB
     */
    public CommitHashMap hashmap = new CommitHashMap(MAX_ITEMS, LOAD_FACTOR);

    public Uncommited uncommited;

    /*
     * WAL related pointers
     */
    public ByteArrayOutputStream baos = new ByteArrayOutputStream();
    public DataOutputStream toWAL = new DataOutputStream(baos);
    public List<TSOHandler.ChannelAndMessage> nextBatch = new ArrayList<TSOHandler.ChannelAndMessage>();

    public TSOState(TimestampOracle timestampOracle) {
        this(null, timestampOracle);
    }

    public TSOState(StateLogger logger, TimestampOracle timestampOracle) {
        this.timestampOracle = timestampOracle;
        this.previousLargestDeletedTimestamp = this.timestampOracle.get();
        this.largestDeletedTimestamp = this.previousLargestDeletedTimestamp;
        this.uncommited = new Uncommited(timestampOracle.first());
        this.logger = logger;
    }

    public void initialize() {
        this.uncommited = new Uncommited(timestampOracle.first());
    }

    public void setLogger(StateLogger logger) {
        this.logger = logger;
    }

    protected TimestampOracle getTimestampOracle() {
        return timestampOracle;
    }

    /**
     * Process commit request.
     * 
     * @param startTimestamp
     */
    protected void processCommit(long startTimestamp, long commitTimestamp) {
        largestDeletedTimestamp = hashmap.setCommitted(startTimestamp, commitTimestamp, largestDeletedTimestamp);
    }

    /**
     * Process abort request.
     * 
     * @param startTimestamp
     */
    protected void processAbort(long startTimestamp) {
        hashmap.setHalfAborted(startTimestamp);
        uncommited.abort(startTimestamp);
    }

    /**
     * Process full abort report.
     * 
     * @param startTimestamp
     */
    protected void processFullAbort(long startTimestamp) {
        hashmap.setFullAborted(startTimestamp);
    }

    /**
     * Process largest deleted timestamp.
     * 
     * @param largestDeletedTimestamp
     */
    protected synchronized void processLargestDeletedTimestamp(long largestDeletedTimestamp) {
        this.largestDeletedTimestamp = Math.max(largestDeletedTimestamp, this.largestDeletedTimestamp);
    }

    /**
     * If logger is disabled, then this call is a noop.
     * 
     * @param record
     * @param cb
     * @param ctx
     */
    public void addRecord(byte[] record, final AddRecordCallback cb, Object ctx) {
        if (logger != null) {
            logger.addRecord(record, cb, ctx);
        } else {
            cb.addRecordComplete(Code.OK, ctx);
        }
    }

    /**
     * Closes this state object.
     */
    void stop() {
        if (logger != null) {
            logger.shutdown();
        }
    }

}
