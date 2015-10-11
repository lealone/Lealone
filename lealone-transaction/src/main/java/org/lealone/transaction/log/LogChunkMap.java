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
package org.lealone.transaction.log;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;

/**
 * A skipList-based log map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class LogChunkMap<K, V> extends ConcurrentSkipListMap<K, V> implements StorageMap<K, V> {
    private static WriteBuffer writeBuffer;

    private static WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }

    private static void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    protected final int id;
    protected final String name;
    protected final DataType keyType;
    protected final DataType valueType;

    protected final LogFileStorage fileStorage;

    private long pos;
    private volatile K lastKey;

    public LogChunkMap(int id, String name, DataType keyType, DataType valueType, Map<String, Object> config) {
        this.id = id;
        this.name = name;
        this.keyType = keyType;
        this.valueType = valueType;

        String storageName = (String) config.get("storageName");
        name = storageName + File.separator + name + LogStorage.MAP_NAME_ID_SEPARATOR + id;
        fileStorage = new LogFileStorage();
        fileStorage.open(name, config);
        pos = fileStorage.size();
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public DataType getKeyType() {
        return keyType;
    }

    @Override
    public DataType getValueType() {
        return valueType;
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public long sizeAsLong() {
        return size();
    }

    @Override
    public void remove() {
        clear();
    }

    @Override
    public K getKey(long index) {
        throw DataUtils.newUnsupportedOperationException("getKey");
    }

    @Override
    public long getKeyIndex(K key) {
        throw DataUtils.newUnsupportedOperationException("getKeyIndex");
    }

    @Override
    public boolean isInMemory() {
        return true;
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        if (a == b) {
            return true;
        } else if (a == null || b == null) {
            return false;
        }
        return valueType.compare(a, b) == 0;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new Cursor<>(this, from);
    }

    @Override
    public void save() {
        WriteBuffer buff = getWriteBuffer();
        K lastKey = this.lastKey;
        for (Entry<K, V> e : tailMap(lastKey).entrySet()) {
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
            releaseWriteBuffer(buff);
        }
        this.lastKey = lastKey;
    }

    @Override
    public void close() {
        save();
        clear();
        fileStorage.close();
    }

    long logChunkSize() {
        return pos;
    }

    private static class Cursor<K, V> implements StorageMapCursor<K, V> {
        private final Iterator<Entry<K, V>> iterator;
        private Entry<K, V> entry;

        Cursor(LogChunkMap<K, V> map, K from) {
            iterator = map.tailMap(from).entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public K next() {
            entry = iterator.next();
            return entry.getKey();
        }

        @Override
        public void remove() {
            iterator.remove();
        }

        @Override
        public K getKey() {
            return entry.getKey();
        }

        @Override
        public V getValue() {
            return entry.getValue();
        }
    }

}
