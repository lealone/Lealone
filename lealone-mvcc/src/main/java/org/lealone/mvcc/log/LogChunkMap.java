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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.memory.MemoryMap;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;

/**
 * A skipList-based log map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class LogChunkMap<K, V> extends MemoryMap<K, V> implements Comparable<LogChunkMap<K, V>> {
    public static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    protected final FileStorage fileStorage;

    final int id;
    private long pos;
    private volatile K lastSyncKey;

    public LogChunkMap(int id, String name, DataType keyType, DataType valueType, Map<String, String> config) {
        super(name, keyType, valueType);
        this.id = id;
        name = getChunkFileName(config, id, name);
        fileStorage = new FileStorage();
        fileStorage.open(name, config);
        pos = fileStorage.size();
        if (pos > 0)
            read();
    }

    static String getChunkFileName(Map<String, String> config, int id, String name) {
        String storageName = config.get("storageName");
        name = storageName + File.separator + name + LogStorage.MAP_NAME_ID_SEPARATOR + id;
        return name;
    }

    @SuppressWarnings("unchecked")
    private void read() {
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            K k = (K) keyType.read(buffer);
            V v = (V) valueType.read(buffer);
            put(k, v);
            lastSyncKey = k;
        }
    }

    @Override
    public synchronized void save() {
        K lastKey = this.lastSyncKey;
        Set<Entry<K, V>> entrySet = lastKey == null ? skipListMap.entrySet() : skipListMap.tailMap(lastKey, false)
                .entrySet();
        if (!entrySet.isEmpty()) {
            WriteBuffer buff = WriteBufferPool.poll();
            try {
                for (Entry<K, V> e : entrySet) {
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

    @Override
    public void close() {
        save();
        super.close();
        fileStorage.close();
    }

    @Override
    public void remove() {
        fileStorage.close();
        fileStorage.delete();
        super.remove();
    }

    long logChunkSize() {
        return pos;
    }

    Set<Entry<K, V>> entrySet() {
        return skipListMap.entrySet();
    }

    K getLastSyncKey() {
        return lastSyncKey;
    }

    @Override
    public int compareTo(LogChunkMap<K, V> o) {
        return this.id - o.id;
    }

    @Override
    public String toString() {
        return "LogChunkMap[" + id + ", " + getName() + "]";
    }
}
