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

package com.codefollower.yourbase.omid.tso.persistence;

import java.nio.ByteBuffer;

import com.codefollower.yourbase.omid.tso.TSOState;
import com.codefollower.yourbase.omid.tso.TimestampOracle;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LoggerProtocol extends TSOState {
    private static final Log LOG = LogFactory.getLog(LoggerProtocol.class);

    /*
     * Protocol flags. Used to identify fields of the logger records.
     */
    public final static byte TIMESTAMP_ORACLE = (byte) -1;
    public final static byte COMMIT = (byte) -2;
    public final static byte LARGEST_DELETED_TIMESTAMP = (byte) -3;
    public final static byte ABORT = (byte) -4;
    public final static byte FULLABORT = (byte) -5;
    public final static byte LOGSTART = (byte) -6;
    public final static byte SNAPSHOT = (byte) -7;

    /**
     * Logger protocol constructor. Currently it only constructs the
     * super class, TSOState.
     * 
     * @param logger
     * @param largestDeletedTimestamp
     */
    LoggerProtocol(TimestampOracle timestampOracle) {
        super(timestampOracle);
    }

    private boolean commits;
    private boolean oracle;
    private boolean aborts;
    private boolean consumed;
    private boolean hasSnapshot;
    private int snapshot = -1;

    /**
     * Execute a logged entry (several logged ops)
     * @param bb Serialized operations
     */
    void execute(ByteBuffer bb) {
        boolean done = !bb.hasRemaining();
        while (!done) {
            byte op = bb.get();
            long timestamp, startTimestamp, commitTimestamp;
            if (LOG.isTraceEnabled()) {
                LOG.trace("Operation: " + op);
            }
            switch (op) {
            case TIMESTAMP_ORACLE:
                timestamp = bb.getLong();
                this.getTimestampOracle().initialize(timestamp);
                this.initialize();
                oracle = true;
                break;
            case COMMIT:
                startTimestamp = bb.getLong();
                commitTimestamp = bb.getLong();
                processCommit(startTimestamp, commitTimestamp);
                if (commitTimestamp < largestDeletedTimestamp) {
                    commits = true;
                }
                break;
            case LARGEST_DELETED_TIMESTAMP:
                timestamp = bb.getLong();
                processLargestDeletedTimestamp(timestamp);

                break;
            case ABORT:
                timestamp = bb.getLong();
                processAbort(timestamp);

                break;
            case FULLABORT:
                timestamp = bb.getLong();
                processFullAbort(timestamp);

                break;
            case LOGSTART:
                consumed = true;
                break;
            case SNAPSHOT:
                int snapshot = (int) bb.getLong();
                if (snapshot > this.snapshot) {
                    this.snapshot = snapshot;
                    this.hasSnapshot = true;
                }
                if (hasSnapshot && snapshot < this.snapshot) {
                    this.aborts = true;
                }
                break;
            }
            if (bb.remaining() == 0)
                done = true;
        }
    }

    /**
     * Checks whether all the required information has been recovered
     * from the log.
     * 
     * @return true if the recovery has finished
     */
    boolean finishedRecovery() {
        return (oracle && commits && aborts) || consumed;
    }

    /**
     * Returns a TSOState object based on this object.
     * 
     * @return
     */
    TSOState getState() {
        return ((TSOState) this);
    }

}
