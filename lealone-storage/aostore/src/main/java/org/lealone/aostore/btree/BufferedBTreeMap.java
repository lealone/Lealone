/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.lealone.aostore.btree;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.lealone.storage.type.DataType;
import org.lealone.storage.type.ObjectDataType;

/**
 * A write optimization BTree stored map.
 * 
 * @param <K> the key class
 * @param <V> the value class
 */
public class BufferedBTreeMap<K, V> extends BTreeMap<K, V> implements Callable<Void> {
    private static Merger merger;
    static {
        merger = new Merger();
        merger.start();
    }

    public static void stopMerger() {
        merger.stopMerger();
    }

    BufferedBTreePage root;

    protected BufferedBTreeMap(DataType keyType, DataType valueType) {
        super(keyType, valueType);
        root = BufferedBTreePage.createEmpty(this, -1);

        merger.addMap(this);
    }

    @Override
    public void commit() {
        root.commit();
    }

    @Override
    public V put(K key, V value) {
        BufferedBTreePage p = root;
        if (p.isLeaf()) {
            p.current.put(key, value);
        } else {
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }

            p.buffers[index].put(key, value);
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        Object v;
        BufferedBTreePage p = root;
        if (p.isLeaf()) {
            v = p.current.get(key);
        } else {
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }

            v = p.buffers[index].get(key);
            if (v == null) {
                BTreePage p2 = p.getChildPage(index);
                v = super.binarySearch(p2, key);
            }
        }
        return (V) v;
    }

    @Override
    public V remove(Object key) {
        V v = get(key);
        if (v != null)
            v = super.remove(key);
        return v;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected BTreePage splitRootIfNeeded(BTreePage p, long writeVersion) {
        if (p.getMemory() <= store.getPageSplitSize() || p.getKeyCount() <= 1) {
            return p;
        }
        int at = p.getKeyCount() / 2;
        long totalCount = p.getTotalCount();
        Object k = p.getKey(at);
        BTreePage split = p.split(at); // p把自己split后被当成左节点
        Object[] keys = { k };
        BTreePage.PageReference[] children = { new BTreePage.PageReference(p, p.getPos(), p.getTotalCount()),
                new BTreePage.PageReference(split, split.getPos(), split.getTotalCount()), };
        BufferedBTreePage p2 = BufferedBTreePage.create(this, writeVersion, keys, null, children, totalCount, 0);
        p2.buffers = new ConcurrentSkipListMap[] { new ConcurrentSkipListMap<>(), new ConcurrentSkipListMap<>() };
        root = p2;
        return p2;
    }

    @Override
    public Void call() throws Exception {
        merge();
        return null;
    }

    @SuppressWarnings("unchecked")
    private void merge() {
        if (root.isLeaf())
            return;

        for (int i = 0; i < root.buffers.length; i++) {
            ConcurrentSkipListMap<Object, Object> buffer = root.buffers[i];
            for (Object key : buffer.keySet()) {
                // 不能先remove再put，因为刚刚remove后，要是在put之前有一个读线程进来，那么它就读不到值了
                Object value = buffer.get(key);
                super.put((K) key, (V) value);
                buffer.remove(key);
            }
        }
    }

    public static class Merger extends Thread {
        private static final ExecutorService executorService = Executors.newCachedThreadPool();

        private volatile boolean isRunning;
        private final ArrayList<BufferedBTreeMap<?, ?>> maps = new ArrayList<>();
        private final ArrayList<Future<Void>> futures = new ArrayList<>();

        public synchronized void addMap(BufferedBTreeMap<?, ?> map) {
            maps.add(map);
        }

        public Merger() {
            super("BTree-Merger");
            setDaemon(true);
        }

        @Override
        public void run() {
            isRunning = true;
            long millis = 5 * 60 * 1000; // TODO 可配置
            while (isRunning) {
                try {
                    sleep(millis);
                } catch (InterruptedException e) {
                    // e.printStackTrace();
                }

                for (BufferedBTreeMap<?, ?> map : maps) {
                    futures.add(executorService.submit(map));
                }

                for (Future<Void> f : futures) {
                    try {
                        f.get();
                    } catch (Exception e) {
                        // e.printStackTrace();
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
                // e.printStackTrace();
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
    public static class Builder<K, V> implements MapBuilder<BufferedBTreeMap<K, V>, K, V> {

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
        public BufferedBTreeMap<K, V> create() {
            if (keyType == null) {
                keyType = new ObjectDataType();
            }
            if (valueType == null) {
                valueType = new ObjectDataType();
            }
            return new BufferedBTreeMap<K, V>(keyType, valueType);
        }
    }
}
