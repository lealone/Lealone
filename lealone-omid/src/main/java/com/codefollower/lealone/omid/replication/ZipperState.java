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

package com.codefollower.lealone.omid.replication;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.codefollower.lealone.omid.tso.TSOMessage;

public class ZipperState implements TSOMessage {

    private long lastStartTimestamp;
    private long lastCommitTimestamp;

    private long lastHalfAbortedTimestamp;
    private long lastFullAbortedTimestamp;

    public ZipperState() {
    }

    public ZipperState(long lastStartTimestamp, long lastCommitTimestamp, long lastHalfAbortedTimestamp,
            long lastFullAbortedTimestamp) {
        this.lastStartTimestamp = lastStartTimestamp;
        this.lastCommitTimestamp = lastCommitTimestamp;
        this.lastHalfAbortedTimestamp = lastHalfAbortedTimestamp;
        this.lastFullAbortedTimestamp = lastFullAbortedTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer buffer) {
        lastStartTimestamp = buffer.readLong();
        lastCommitTimestamp = buffer.readLong();

        lastHalfAbortedTimestamp = buffer.readLong();
        lastFullAbortedTimestamp = buffer.readLong();
    }

    @Override
    public void writeObject(DataOutputStream buffer) throws IOException {
        buffer.writeLong(lastStartTimestamp);
        buffer.writeLong(lastCommitTimestamp);
        buffer.writeLong(lastHalfAbortedTimestamp);
        buffer.writeLong(lastFullAbortedTimestamp);
    }

    public long getLastStartTimestamp() {
        return lastStartTimestamp;
    }

    public void setLastStartTimestamp(long lastStartTimestamp) {
        this.lastStartTimestamp = lastStartTimestamp;
    }

    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public void setLastCommitTimestamp(long lastCommitTimestamp) {
        this.lastCommitTimestamp = lastCommitTimestamp;
    }

    public long getLastHalfAbortedTimestamp() {
        return lastHalfAbortedTimestamp;
    }

    public void setLastHalfAbortedTimestamp(long lastHalfAbortedTimestamp) {
        this.lastHalfAbortedTimestamp = lastHalfAbortedTimestamp;
    }

    public long getLastFullAbortedTimestamp() {
        return lastFullAbortedTimestamp;
    }

    public void setLastFullAbortedTimestamp(long lastFullAbortedTimestamp) {
        this.lastFullAbortedTimestamp = lastFullAbortedTimestamp;
    }

}
