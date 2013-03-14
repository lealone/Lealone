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

public class AbortedTransaction {
    private long startTimestamp;
    private long snapshot;

    public AbortedTransaction(long startTimestamp, long snapshot) {
        this.startTimestamp = startTimestamp;
        this.snapshot = snapshot;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(long snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
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
        AbortedTransaction other = (AbortedTransaction) obj;
        if (startTimestamp != other.startTimestamp)
            return false;
        return true;
    }

}
