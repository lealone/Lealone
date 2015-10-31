/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.cluster.streaming;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import org.lealone.storage.StorageMap;

/**
 * StreamWriter writes given section of the StorageMap to given channel.
 */
public class StreamWriter {
    // private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    protected final StorageMap<Object, Object> map;
    protected final Object firstKey;
    protected final Object lastKey;

    // protected final StreamSession session;
    // protected final StreamRateLimiter limiter;

    public StreamWriter(StorageMap<Object, Object> map, Object firstKey, Object lastKey, StreamSession session) {
        this.map = map;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        // this.session = session;
        //
        // this.limiter = StreamManager.getRateLimiter(session.peer);
    }

    public void write(WritableByteChannel target) throws IOException {
        map.transferTo(target, firstKey, lastKey);
    }
}
