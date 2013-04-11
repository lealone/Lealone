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

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public class ReadersAwareBuffer {
    private static final int CAPACITY = 1024 * 1024;
    public static long nBuffers;

    public final ChannelBuffer buffer;
    private int pendingReaders = 0;
    private boolean scheduledForPool;

    public ReadersAwareBuffer() {
        nBuffers++;
        buffer = ChannelBuffers.directBuffer(CAPACITY);
    }

    public synchronized void increaseReaders() {
        pendingReaders++;
    }

    public synchronized void decreaseReaders() {
        pendingReaders--;
    }

    public synchronized boolean isReadyForPool() {
        return scheduledForPool && pendingReaders == 0;
    }

    public synchronized void scheduleForPool() {
        scheduledForPool = true;
    }

    public synchronized void reset() {
        pendingReaders = 0;
        scheduledForPool = false;
    }
}
