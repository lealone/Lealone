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

package com.codefollower.yourbase.omid.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.codefollower.yourbase.omid.tso.TSOMessage;

public class CommitQueryResponse implements TSOMessage {
    public long startTimestamp;
    public long commitTimestamp;
    public long queryTimestamp;
    //if retry is set, committed value is not valid
    //then Tx should read again, if the commit time stamp was at HBaseServer it is committed, otherwise it is aborted
    //refer to proposal for more details
    public boolean retry;
    //committed is unvalid if retry is set
    public boolean committed;

    public CommitQueryResponse() {
        //the defauls are false and 0, no need to set again
    }

    public CommitQueryResponse(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public CommitQueryResponse(long startTimestamp, long queryTimestamp, boolean committed) {
        this.startTimestamp = startTimestamp;
        this.queryTimestamp = queryTimestamp;
        this.committed = committed;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) {
        startTimestamp = aInputStream.readLong();
        queryTimestamp = aInputStream.readLong();
        commitTimestamp = aInputStream.readLong();
        committed = aInputStream.readByte() == 1;
        retry = aInputStream.readByte() == 1;
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(startTimestamp);
        aOutputStream.writeLong(queryTimestamp);
        aOutputStream.writeLong(commitTimestamp);
        aOutputStream.writeByte(committed ? 1 : 0);
        aOutputStream.writeByte(retry ? 1 : 0);
    }

    @Override
    public String toString() {
        return "CommitQueryResponse[" + startTimestamp + "," + queryTimestamp + "," + committed + "," + commitTimestamp + "]";
    }
}
