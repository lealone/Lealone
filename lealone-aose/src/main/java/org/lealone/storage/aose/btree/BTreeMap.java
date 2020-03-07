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
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DataUtils;
import org.lealone.db.IDatabase;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.PageKey;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.PageOperations.Get;
import org.lealone.storage.aose.btree.PageOperations.Put;
import org.lealone.storage.aose.btree.PageOperations.PutIfAbsent;
import org.lealone.storage.aose.btree.PageOperations.Remove;
import org.lealone.storage.aose.btree.PageOperations.Replace;
import org.lealone.storage.type.StorageDataType;

/**
 * 支持同步和异步风格的BTree.
 * 
 * <p>
 * 对于写操作，使用同步风格的API时会阻塞线程，异步风格的API不阻塞线程.
 * <p>
 * 对于读操作，不阻塞线程，允许多线程对BTree进行读取操作.
 * 
 * @param <K> the key class
 * @param <V> the value class
 */
public class BTreeMap<K, V> extends StorageMapBase<K, V> {

    // 只允许通过成员方法访问这个特殊的字段
    private final AtomicLong size = new AtomicLong(0);

    protected final boolean readOnly;
    protected final Map<String, Object> config;
    protected final BTreeStorage btreeStorage;
    protected final PageOperationHandlerFactory pohFactory;
    // 每个btree固定一个处理器用于处理node page的所有状态更新操作
    protected final PageOperationHandler nodePageOperationHandler;
    protected PageStorageMode pageStorageMode = PageStorageMode.ROW_STORAGE;

    // btree的root page，最开始是一个leaf page，随时都会指向新的page
    protected volatile BTreePage root;
    protected volatile boolean parallelDisabled;

    @SuppressWarnings("unchecked")
    protected BTreeMap(String name, StorageDataType keyType, StorageDataType valueType, Map<String, Object> config,
            AOStorage aoStorage) {
        super(name, keyType, valueType, aoStorage);
        DataUtils.checkArgument(config != null, "The config may not be null");

        this.readOnly = config.containsKey("readOnly");
        this.config = config;
        this.pohFactory = aoStorage.getPageOperationHandlerFactory();
        this.nodePageOperationHandler = pohFactory.getNodePageOperationHandler();
        Object mode = config.get("pageStorageMode");
        if (mode != null) {
            pageStorageMode = PageStorageMode.valueOf(mode.toString());
        }

        btreeStorage = new BTreeStorage((BTreeMap<Object, Object>) this);

        if (btreeStorage.lastChunk != null) {
            root = btreeStorage.readPage(btreeStorage.lastChunk.rootPagePos);
            setMaxKey(lastKey());
            size.set(btreeStorage.lastChunk.mapSize);
        } else {
            root = BTreeLeafPage.createEmpty(this);
        }
        disableParallelIfNeeded();
    }

    protected BTreeMap(BTreeMap<?, ?> map) {
        super(map.name, map.keyType, map.valueType, map.storage);
        size.set(map.size.get());
        readOnly = map.readOnly;
        config = map.config;
        btreeStorage = map.btreeStorage;
        pohFactory = map.pohFactory;
        nodePageOperationHandler = map.nodePageOperationHandler;
        pageStorageMode = map.pageStorageMode;
        root = map.root;
        parallelDisabled = map.parallelDisabled;
    }

    private void disableParallelIfNeeded() {
        if (root.isLeaf())
            parallelDisabled = true;
    }

    void enableParallelIfNeeded() {
        if (parallelDisabled && root.isNode() && root.getRawChildPageCount() >= 2) {
            parallelDisabled = false;
        }
    }

    @Override
    public V get(K key) {
        return binarySearch(key, true);
    }

    public V get(K key, boolean allColumns) {
        return binarySearch(key, allColumns);
    }

    public V get(K key, int columnIndex) {
        return binarySearch(key, new int[] { columnIndex });
    }

    @Override
    public V get(K key, int[] columnIndexes) {
        return binarySearch(key, columnIndexes);
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, boolean allColumns) {
        BTreePage p = root.gotoLeafPage(key);
        p = p.redirectIfSplited(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, allColumns) : null;
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, int[] columnIndexes) {
        BTreePage p = root.gotoLeafPage(key);
        p = p.redirectIfSplited(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, columnIndexes) : null;
    }

    // 如果map是只读的或者已经关闭了就不能再写了，并且不允许值为null
    private void checkWrite(V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");
        checkWrite();
    }

    // 有子类用到
    protected void checkWrite() {
        if (btreeStorage.isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException("This map is read-only");
        }
    }

    @SuppressWarnings("unchecked")
    private <R> PageOperation.Listener<R> getPageOperationListener() {
        Object object = Thread.currentThread();
        PageOperation.Listener<R> listener;
        if (object instanceof PageOperation.Listener)
            listener = (PageOperation.Listener<R>) object;
        else
            listener = new PageOperation.SyncListener<R>();
        return listener;
    }

    @Override
    public V put(K key, V value) {
        PageOperation.Listener<V> listener = getPageOperationListener();
        put(key, value, listener);
        return listener.await();
    }

    @Override
    public V putIfAbsent(K key, V value) {
        PageOperation.Listener<V> listener = getPageOperationListener();
        putIfAbsent(key, value, listener);
        return listener.await();
    }

    @Override
    public V remove(K key) {
        PageOperation.Listener<V> listener = getPageOperationListener();
        remove(key, listener);
        return listener.await();
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        PageOperation.Listener<Boolean> listener = getPageOperationListener();
        replace(key, oldValue, newValue, listener);
        return listener.await();
    }

    // 以下API子类会覆盖
    protected IDatabase getDatabase() {
        return null;
    }

    protected void fireLeafPageSplit(Object splitKey) {
    }

    protected void fireLeafPageRemove(PageKey pageKey, BTreePage leafPage) {
    }

    protected <R> Object putRemote(BTreePage p, K key, V value, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
        return null;
    }

    protected <R> Object removeRemote(BTreePage p, K key, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
        return null;
    }

    /**
     * Use the new root page from now on.
     * 
     * @param newRoot the new root page
     */
    protected void newRoot(BTreePage newRoot) {
        if (root != newRoot) {
            root = newRoot;
        }
    }

    boolean isShardingMode() {
        return false;
    }

    @Override
    public K firstKey() {
        return getFirstLast(true);
    }

    @Override
    public K lastKey() {
        return getFirstLast(false);
    }

    /**
     * Get the first (lowest) or last (largest) key.
     * 
     * @param first whether to retrieve the first key
     * @return the key, or null if the map is empty
     */
    @SuppressWarnings("unchecked")
    protected K getFirstLast(boolean first) {
        if (size() == 0) {
            return null;
        }
        BTreePage p = root;
        while (true) {
            if (p.isLeaf()) {
                p = p.redirectIfSplited(first);
                return (K) p.getKey(first ? 0 : p.getKeyCount() - 1);
            }
            p = p.getChildPage(first ? 0 : getChildPageCount(p) - 1);
        }
    }

    @Override
    public K lowerKey(K key) {
        return getMinMax(key, true, true);
    }

    @Override
    public K floorKey(K key) {
        return getMinMax(key, true, false);
    }

    @Override
    public K higherKey(K key) {
        return getMinMax(key, false, true);
    }

    @Override
    public K ceilingKey(K key) {
        return getMinMax(key, false, false);
    }

    /**
     * Get the smallest or largest key using the given bounds.
     * 
     * @param key the key
     * @param min whether to retrieve the smallest key
     * @param excluding if the given upper/lower bound is exclusive
     * @return the key, or null if no such key exists
     */
    protected K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(BTreePage p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
            p = p.redirectIfSplited(key);
            int x = p.binarySearch(key);
            if (x < 0) {
                x = -x - (min ? 2 : 1);
            } else if (excluding) {
                x += min ? -1 : 1;
            }
            if (x < 0 || x >= p.getKeyCount()) {
                return null;
            }
            return (K) p.getKey(x);
        }
        int x = p.binarySearch(key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
        }
        while (true) {
            if (x < 0 || x >= getChildPageCount(p)) {
                return null;
            }
            K k = getMinMax(p.getChildPage(x), key, min, excluding);
            if (k != null) {
                return k;
            }
            x += min ? -1 : 1;
        }
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
    public long size() {
        return size.get();
    }

    void incrementSize() {
        size.incrementAndGet();
    }

    void decrementSize() {
        size.decrementAndGet();
    }

    void resetSize() {
        size.set(0);
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return cursor(IterationParameters.create(from));
    }

    @Override
    public StorageMapCursor<K, V> cursor(IterationParameters<K> parameters) {
        if (parameters.pageKeys == null)
            return new BTreeCursor<>(this, root, parameters);
        else
            return new PageKeyCursor<>(root, parameters);
    }

    @Override
    public synchronized void clear() {
        checkWrite();
        List<String> replicationHostIds = root.getReplicationHostIds();
        root.removeAllRecursive();
        size.set(0);
        newRoot(BTreeLeafPage.createEmpty(this));
        disableParallelIfNeeded();
        root.setReplicationHostIds(replicationHostIds);
    }

    @Override
    public synchronized void remove() {
        btreeStorage.remove();
        closeMap();
    }

    @Override
    public boolean isClosed() {
        return btreeStorage.isClosed();
    }

    @Override
    public synchronized void close() {
        closeMap();
        btreeStorage.close();
    }

    private void closeMap() {
        storage.closeMap(name);
    }

    @Override
    public synchronized void save() {
        btreeStorage.save();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public BTreeStorage getBTreeStorage() {
        return btreeStorage;
    }

    /**
     * Get the child page count for this page. This is to allow another map
     * implementation to override the default, in case the last child is not to be used.
     * 
     * @param p the page
     * @return the number of direct children
     */
    protected int getChildPageCount(BTreePage p) {
        return p.getRawChildPageCount();
    }

    protected String getType() {
        return "BTree";
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        DataUtils.appendMap(buff, "name", name);
        String type = getType();
        if (type != null) {
            DataUtils.appendMap(buff, "type", type);
        }
        return buff.toString();
    }

    public void printPage() {
        printPage(true);
    }

    public void printPage(boolean readOffLinePage) {
        System.out.println(root.getPrettyPageInfo(readOffLinePage));
    }

    @Override
    public long getDiskSpaceUsed() {
        return btreeStorage.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return btreeStorage.getMemorySpaceUsed();
    }

    public BTreePage getRootPage() {
        return root;
    }

    public void setPageStorageMode(PageStorageMode pageStorageMode) {
        this.pageStorageMode = pageStorageMode;
    }

    public BTreePage gotoLeafPage(Object key) {
        return root.gotoLeafPage(key);
    }

    public synchronized void setRootPage(ByteBuffer buff) {
        root = BTreePage.readReplicatedPage(this, buff);
        if (root.isNode() && !getName().endsWith("_0")) { // 只异步读非SYS表
            root.readRemotePages();
        }
    }
    //////////////////// 以下是异步API的实现 ////////////////////////////////

    @Override
    public void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        Get<K, V> get = new Get<>(this, key, handler);
        pohFactory.addPageOperation(get);
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        Put<K, V, V> put = new Put<>(this, key, value, handler);
        pohFactory.addPageOperation(put);
    }

    @Override
    public PageOperation createPutOperation(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        return new Put<>(this, key, value, handler);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        PutIfAbsent<K, V> putIfAbsent = new PutIfAbsent<>(this, key, value, handler);
        pohFactory.addPageOperation(putIfAbsent);
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        checkWrite(newValue);
        Replace<K, V> replace = new Replace<>(this, key, oldValue, newValue, handler);
        pohFactory.addPageOperation(replace);
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite();
        Remove<K, V> remove = new Remove<>(this, key, handler);
        pohFactory.addPageOperation(remove);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K append(V value, AsyncHandler<AsyncResult<V>> handler) {
        // 先得到一个long类型的key，再调用put
        K key = (K) ValueLong.get(maxKey.incrementAndGet());
        put(key, value, handler);
        return key;
    }
}
