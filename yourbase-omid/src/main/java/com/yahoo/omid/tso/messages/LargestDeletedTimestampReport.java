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

package com.yahoo.omid.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object that notifies clients about and increase of the largest deleted timestamp
 * 
 */
public class LargestDeletedTimestampReport implements TSOMessage {
    /**
     * Starting timestamp
     */
    public long largestDeletedTimestamp;

    public LargestDeletedTimestampReport() {
    }

    public LargestDeletedTimestampReport(long largestDeletedTimestamp) {
        this.largestDeletedTimestamp = largestDeletedTimestamp;
    }

    @Override
    public String toString() {
        return "Largest Deleted Timestamp Report: T_s:" + largestDeletedTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) {

        largestDeletedTimestamp = aInputStream.readLong();
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(largestDeletedTimestamp);
    }

    @Override
    public void writeObject(ChannelBuffer buffer) {
        buffer.writeLong(largestDeletedTimestamp);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (largestDeletedTimestamp ^ (largestDeletedTimestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LargestDeletedTimestampReport other = (LargestDeletedTimestampReport) obj;
        if (largestDeletedTimestamp != other.largestDeletedTimestamp)
            return false;
        return true;
    }

}
