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
package org.lealone.mvstore;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.mvstore.type.DataType;
import org.lealone.mvstore.type.ObjectDataType;

public class LockFreeMVMap<K, V> extends MVMap<K, V> implements Callable<Void> {

    private static Merger merger;
    static {
        merger = new Merger();
        merger.start();
    }

    public static void stopMerger() {
        merger.stopMerger();
    }

    private static class ValueHolder<V> {
        final V value;

        ValueHolder(V value) {
            this.value = value;
        }
    }

    private volatile ConcurrentSkipListMap<K, ValueHolder<V>> current = new ConcurrentSkipListMap<>();

    //snapshot里的数据是临时只读的
    private volatile ConcurrentSkipListMap<K, ValueHolder<V>> snapshot;

    public LockFreeMVMap(DataType keyType, DataType valueType) {
        super(keyType, valueType);

        merger.addMap(this);
    }

    @Override
    public V put(K key, V value) {
        ValueHolder<V> vh = current.put(key, new ValueHolder<V>(value));
        if (vh != null)
            return vh.value;

        return null;
    }

    @Override
    public V get(Object key) {
        ValueHolder<V> vh = current.get(key);
        if (vh != null)
            return vh.value;

        if (snapshot != null) {
            vh = snapshot.get(key);
            if (vh != null)
                return vh.value;
        }

        return super.get(key);
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        //按旧到新的次序找，B-Tree中最旧，其次是snapshot，最新是current
        V old = super.get(key);
        if (old != null)
            current.put((K) key, new ValueHolder<V>(null));

        ValueHolder<V> vh;

        if (snapshot != null) {
            vh = snapshot.get(key);
            if (vh != null && vh.value != null) {
                current.put((K) key, new ValueHolder<V>(null));
                old = vh.value;
            }
        }

        vh = current.remove(key);
        if (vh != null && vh.value != null)
            old = vh.value;

        return old;
    }

    @Override
    public V replace(K key, V value) {
        V old = super.get(key);
        if (old != null)
            current.put(key, new ValueHolder<V>(value));

        ValueHolder<V> vh;

        if (snapshot != null) {
            vh = snapshot.get(key);
            if (vh != null && vh.value != null) {
                current.put(key, new ValueHolder<V>(value));
                old = vh.value;
            }
        }

        vh = current.replace(key, new ValueHolder<V>(value));
        if (vh != null && vh.value != null)
            old = vh.value;

        return old;
    }

    @Override
    public int size() {
        int size = super.size();
        size += size(current);
        size += size(snapshot);
        return size;
    }

    private int size(ConcurrentSkipListMap<K, ValueHolder<V>> map) {
        if (map == null)
            return 0;
        int size = 0;
        for (ValueHolder<V> vh : map.values()) {
            if (vh.value != null)
                size++;
        }
        return size;
    }

    @Override
    public Void call() throws Exception {
        beginMerge();
        return null;
    }

    private void beginMerge() {
        if (current.isEmpty())
            return;

        snapshot = current;
        current = new ConcurrentSkipListMap<K, ValueHolder<V>>();
        merge();
        snapshot = null;
    }

    private void merge() {
        beforeWrite();

        long v = writeVersion;
        Page p = root.copy(v);
        Object key;
        Object value;

        for (Entry<K, ValueHolder<V>> e : snapshot.entrySet()) {
            key = e.getKey();
            value = e.getValue().value;
            if (value != null) {
                p = splitRootIfNeeded(p, v);
                put(p, v, key, value);
            } else {
                remove(p, v, key);
                if (!p.isLeaf() && p.getTotalCount() == 0) {
                    p.removePage();
                    p = Page.createEmpty(this, p.getVersion());
                }
            }
        }
        newRoot(p);
        store.commit();
    }

    public static class Merger extends Thread {
        private static final ExecutorService executorService = Executors.newCachedThreadPool();

        private volatile boolean isRunning;
        private final ArrayList<LockFreeMVMap<?, ?>> maps = new ArrayList<>();
        private final ArrayList<Future<Void>> futures = new ArrayList<>();

        public synchronized void addMap(LockFreeMVMap<?, ?> map) {
            maps.add(map);
        }

        public Merger() {
            super("BTree-Merger");
        }

        @Override
        public void run() {
            isRunning = true;
            long millis = 5 * 60 * 1000; //TODO 可配置
            while (isRunning) {
                try {
                    sleep(millis);
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                }

                for (LockFreeMVMap<?, ?> map : maps) {
                    futures.add(executorService.submit(map));
                }

                for (Future<Void> f : futures) {
                    try {
                        f.get();
                    } catch (Exception e) {
                        //e.printStackTrace();
                    }
                }

                futures.clear();
            }
        }

        public void stopMerger() {
            isRunning = false;
            interrupt();
            try {
                join();
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }

            executorService.shutdown();
        }
    }

    /**
     * A builder for this class.
     *
     * @param <K> the key type
     * @param <V> the value type
     */
    public static class Builder<K, V> implements MapBuilder<LockFreeMVMap<K, V>, K, V> {

        protected DataType keyType;
        protected DataType valueType;

        /**
         * Create a new builder with the default key and value data types.
         */
        public Builder() {
            // ignore
        }

        /**
         * Set the key data type.
         *
         * @param keyType the key type
         * @return this
         */
        public Builder<K, V> keyType(DataType keyType) {
            this.keyType = keyType;
            return this;
        }

        /**
         * Set the key data type.
         *
         * @param valueType the key type
         * @return this
         */
        public Builder<K, V> valueType(DataType valueType) {
            this.valueType = valueType;
            return this;
        }

        @Override
        public LockFreeMVMap<K, V> create() {
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new LockFreeMVMap<K, V>(keyType, valueType);
        }
    }
}