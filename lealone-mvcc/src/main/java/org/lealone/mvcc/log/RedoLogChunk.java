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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.mvcc.log.LogStorage;
import org.lealone.mvcc.log.RedoLogChunk;
import org.lealone.mvcc.log.RedoLogKeyType;
import org.lealone.mvcc.log.RedoLogValue;
import org.lealone.mvcc.log.RedoLogValueType;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;

/**
 * A skipList-based redo log chunk
 *  
 * @author zhh
 */
class RedoLogChunk implements Comparable<RedoLogChunk> {

    private static final String CHUNK_FILE_NAME_PREFIX = "redoLog" + LogStorage.NAME_ID_SEPARATOR;

    private static String getChunkFileName(Map<String, String> config, int id) {
        String storageName = config.get("storageName");
        return storageName + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private static class KeyComparator<K> implements java.util.Comparator<K> {
        DataType keyType;

        public KeyComparator(DataType keyType) {
            this.keyType = keyType;
        }

        @Override
        public int compare(K k1, K k2) {
            return keyType.compare(k1, k2);
        }
    }

    private final int id;
    private final DataType keyType;
    private final DataType valueType;
    private final ConcurrentSkipListMap<Long, RedoLogValue> skipListMap;
    private final FileStorage fileStorage;

    private long pos;
    private volatile Long lastSyncKey;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        // 不使用ObjectDataType，因为ObjectDataType需要自动侦测，会有一些开销
        this.keyType = new RedoLogKeyType();
        this.valueType = new RedoLogValueType();
        skipListMap = new ConcurrentSkipListMap<>(new KeyComparator<Long>(keyType));
        String chunkFileName = getChunkFileName(config, id);
        fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, config);
        pos = fileStorage.size();
        if (pos > 0)
            read();
    }

    private void read() {
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            Long k = (Long) keyType.read(buffer);
            RedoLogValue v = (RedoLogValue) valueType.read(buffer);
            skipListMap.put(k, v);
            lastSyncKey = k;
        }
    }

    int getId() {
        return id;
    }

    RedoLogValue put(Long key, RedoLogValue value) {
        return skipListMap.put(key, value);
    }

    Iterator<Entry<Long, RedoLogValue>> cursor(Long from) {
        return from == null ? skipListMap.entrySet().iterator() : skipListMap.tailMap(from).entrySet().iterator();
    }

    Set<Entry<Long, RedoLogValue>> entrySet() {
        return skipListMap.entrySet();
    }

    void close() {
        save();
        fileStorage.close();
    }

    synchronized void save() {
        Long lastKey = this.lastSyncKey;
        Set<Entry<Long, RedoLogValue>> entrySet = lastKey == null ? skipListMap.entrySet() : skipListMap.tailMap(
                lastKey, false).entrySet();
        if (!entrySet.isEmpty()) {
            WriteBuffer buff = WriteBufferPool.poll();
            try {
                for (Entry<Long, RedoLogValue> e : entrySet) {
                    lastKey = e.getKey();
                    keyType.write(buff, lastKey);
                    valueType.write(buff, e.getValue());
                }
                int chunkLength = buff.position();
                if (chunkLength > 0) {
                    buff.limit(chunkLength);
                    buff.position(0);
                    fileStorage.writeFully(pos, buff.getBuffer());
                    pos += chunkLength;
                    fileStorage.sync();
                }
                this.lastSyncKey = lastKey;
            } finally {
                WriteBufferPool.offer(buff);
            }
        }
    }

    long logChunkSize() {
        return pos;
    }

    Long lastKey() {
        try {
            return skipListMap.lastKey();
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    Long getLastSyncKey() {
        return lastSyncKey;
    }

    @Override
    public int compareTo(RedoLogChunk o) {
        return Integer.signum(this.id - o.id);
    }

    @Override
    public String toString() {
        return "RedoLogChunk[" + id + ", " + fileStorage.getFileName() + "]";
    }
}
