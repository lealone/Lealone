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
import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.streaming.messages.FileMessageHeader;
import org.lealone.storage.StorageMap;

/**
 * StreamReader reads from stream and writes to SSTable.
 */
public class StreamReader {
    protected final String mapName;
    protected final long estimatedKeys;
    protected final List<Range<Token>> ranges;
    protected final StreamSession session;
    protected final StorageMap<Object, Object> map;

    public StreamReader(FileMessageHeader header, StreamSession session, StorageMap<Object, Object> map) {
        this.session = session;
        this.mapName = header.mapName;
        this.estimatedKeys = header.estimatedKeys;
        this.ranges = header.ranges;
        this.map = map;
    }

    public void read(ReadableByteChannel channel) throws IOException {
        // map.transferFrom(channel);
    }
}
