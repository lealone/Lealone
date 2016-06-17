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
package org.lealone.mvcc.log;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.lealone.mvcc.log.RedoLogChunk;
import org.lealone.mvcc.log.RedoLogValue;

/**
 * A redo log
 *
 * @author zhh
 */
public class RedoLog {

    private static final long DEFAULT_LOG_CHUNK_SIZE = 32 * 1024 * 1024;

    private final Map<String, String> config;
    private final long logChunkSize;

    private RedoLogChunk current;

    public RedoLog(int id, Map<String, String> config) {
        this.config = config;
        if (config.containsKey("log_chunk_size"))
            logChunkSize = Long.parseLong(config.get("log_chunk_size"));
        else
            logChunkSize = DEFAULT_LOG_CHUNK_SIZE;

        current = new RedoLogChunk(id, config);
    }

    public RedoLogValue put(Long key, RedoLogValue value) {
        return current.put(key, value);
    }

    public Iterator<Entry<Long, RedoLogValue>> cursor(Long from) {
        return current.cursor(from);
    }

    public Set<Entry<Long, RedoLogValue>> entrySet() {
        return current.entrySet();
    }

    public void close() {
        save();
        current.close();
    }

    public void save() {
        current.save();
        if (current.logChunkSize() > logChunkSize) {
            current.close();
            current = new RedoLogChunk(current.getId() + 1, config);
        }
    }

    public Long lastKey() {
        return current.lastKey();
    }

    public Long getLastSyncKey() {
        return current.getLastSyncKey();
    }

}
