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
package org.lealone.storage.aose;

import org.lealone.storage.DelegatedStorageMap;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageMapCursor;

/**
 * An adaptive optimization map
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author zhh
 */
public class AOMap<K, V> extends DelegatedStorageMap<K, V> {
    private final Object sync = new Object();
    private volatile BufferedMap<K, V> bmap;

    private volatile boolean writing;
    private volatile boolean switching;

    private volatile boolean waitWriteFinish;

    // 只是两个近似值，不需要同步
    private int readCount;
    private int writeCount;

    public AOMap(StorageMap<K, V> map) {
        super(map);
        switchToBufferedMap(); // 默认最开始就使用BufferedMap
    }

    private void waitWriteFinishIfNeeded() {
        waitWriteFinish = true;
        if (writing) {
            synchronized (sync) {
                while (writing) {
                    try {
                        sync.wait();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }
        switching = true;
        waitWriteFinish = false;
    }

    public void switchToBufferedMap() {
        waitWriteFinishIfNeeded();
        synchronized (sync) {
            try {
                bmap = new BufferedMap<>(map);
                map = bmap;
                AOStorageService.addBufferedMap(bmap);
                resetCount();
            } finally {
                switching = false;
                sync.notifyAll();
            }
        }
    }

    public void switchToNoBufferedMap() {
        waitWriteFinishIfNeeded();
        synchronized (sync) {
            try {
                AOStorageService.removeBufferedMap(bmap);
                bmap.merge();
                map = bmap.getMap();
                resetCount();
            } finally {
                switching = false;
                sync.notifyAll();
            }
        }
    }

    private void beforeWrite() {
        if (switching) {
            synchronized (sync) {
                while (switching) {
                    try {
                        sync.wait();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
            }
        }

        writing = true;
        writeCount++;
    }

    private void afterWrite() {
        writing = false;
        if (waitWriteFinish) {
            synchronized (sync) {
                sync.notifyAll();
            }
        }
    }

    private void resetCount() {
        readCount = 1;
        writeCount = 1;
    }

    public int getReadPercent() {
        long total = readCount + writeCount;
        double result = readCount / total;
        return (int) (result * 100);
    }

    public int getWritePercent() {
        long total = readCount + writeCount;
        double result = writeCount / total;
        return (int) (result * 100);
    }

    @Override
    public V get(K key) {
        readCount++;
        return map.get(key);
    }

    @Override
    public V put(K key, V value) {
        beforeWrite();
        try {
            return map.put(key, value);
        } finally {
            afterWrite();
        }
    }

    @Override
    public V putIfAbsent(K key, V value) {
        beforeWrite();
        try {
            return map.putIfAbsent(key, value);
        } finally {
            afterWrite();
        }

    }

    @Override
    public V remove(K key) {
        beforeWrite();
        try {
            return map.remove(key);
        } finally {
            afterWrite();
        }
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        beforeWrite();
        try {
            return map.replace(key, oldValue, newValue);
        } finally {
            afterWrite();
        }
    }

    @Override
    public K firstKey() {
        readCount++;
        return map.firstKey();
    }

    @Override
    public K lastKey() {
        readCount++;
        return map.lastKey();
    }

    @Override
    public K lowerKey(K key) {
        readCount++;
        return map.lowerKey(key);
    }

    @Override
    public K floorKey(K key) {
        readCount++;
        return map.floorKey(key);
    }

    @Override
    public K higherKey(K key) {
        readCount++;
        return map.higherKey(key);
    }

    @Override
    public K ceilingKey(K key) {
        readCount++;
        return map.ceilingKey(key);
    }

    @Override
    public boolean areValuesEqual(Object a, Object b) {
        return map.areValuesEqual(a, b);
    }

    @Override
    public boolean containsKey(K key) {
        readCount++;
        return map.containsKey(key);
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        readCount++;
        return map.cursor(from);
    }

    @Override
    public void close() {
        map.close();
        AOStorageService.removeAOMap(this);
    }

    @Override
    public K append(V value) {
        beforeWrite();
        try {
            return map.append(value);
        } finally {
            afterWrite();
        }
    }
}
