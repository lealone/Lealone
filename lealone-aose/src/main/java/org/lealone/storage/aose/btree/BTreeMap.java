/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lealone.common.util.DataUtils;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.CursorParameters;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.aose.btree.page.LeafPage;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageKeyCursor;
import org.lealone.storage.aose.btree.page.PageOperations.Append;
import org.lealone.storage.aose.btree.page.PageOperations.Put;
import org.lealone.storage.aose.btree.page.PageOperations.PutIfAbsent;
import org.lealone.storage.aose.btree.page.PageOperations.Remove;
import org.lealone.storage.aose.btree.page.PageOperations.Replace;
import org.lealone.storage.aose.btree.page.PageOperations.SingleWrite;
import org.lealone.storage.aose.btree.page.PageReference;
import org.lealone.storage.aose.btree.page.PageStorageMode;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.storage.page.PageOperationHandlerFactory;
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
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final boolean readOnly;
    private final boolean inMemory;
    private final Map<String, Object> config;
    private final BTreeStorage btreeStorage;
    private final PageOperationHandlerFactory pohFactory;
    private PageStorageMode pageStorageMode = PageStorageMode.ROW_STORAGE;

    private class RootPageReference extends PageReference {
        @Override
        public void replacePage(Page page) {
            super.replacePage(page);
            setRootRef(page); // 当要替换page时也设置root page相关信息
        }
    }

    private final RootPageReference rootRef = new RootPageReference();
    // btree的root page，最开始是一个leaf page，随时都会指向新的page
    private Page root;

    public BTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, Object> config, AOStorage aoStorage) {
        super(name, keyType, valueType, aoStorage);
        DataUtils.checkNotNull(config, "config");
        // 只要包含就为true
        readOnly = config.containsKey("readOnly");
        inMemory = config.containsKey("inMemory");

        this.config = config;
        this.pohFactory = (PageOperationHandlerFactory) config.get("pohFactory");
        Object mode = config.get("pageStorageMode");
        if (mode != null) {
            pageStorageMode = PageStorageMode.valueOf(mode.toString());
        }

        btreeStorage = new BTreeStorage(this);
        Chunk lastChunk = btreeStorage.getLastChunk();
        if (lastChunk != null) {
            size.set(lastChunk.mapSize);
            Page root = btreeStorage.readPage(lastChunk.rootPagePos);
            // 提前设置，如果root page是node类型，子page就能在Page.getChildPage中找到ParentRef
            setRootRef(root);
            setMaxKey(lastKey());
        } else {
            root = LeafPage.createEmpty(this);
            setRootRef(root);
        }
    }

    private void setRootRef(Page root) {
        if (this.root != root) {
            this.root = root;
        }
        if (rootRef.getPage() != root) {
            if (root.getRef() != rootRef) {
                root.setRef(rootRef);
                rootRef.replacePage(root);
            }
            if (root.isNode()) {
                for (PageReference ref : root.getChildren()) {
                    Page p = ref.getPage();
                    if (p != null && p.getParentRef() != rootRef)
                        p.setParentRef(rootRef);
                }
            }
        }
    }

    public Page getRootPage() {
        return root;
    }

    public void newRoot(Page newRoot) {
        setRootRef(newRoot);
    }

    private void acquireSharedLock() {
        lock.readLock().lock();
    }

    private void releaseSharedLock() {
        lock.readLock().unlock();
    }

    private void acquireExclusiveLock() {
        lock.writeLock().lock();
    }

    private void releaseExclusiveLock() {
        lock.writeLock().unlock();
    }

    public PageOperationHandlerFactory getPohFactory() {
        return pohFactory;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public Object getConfig(String key) {
        return config.get(key);
    }

    public BTreeStorage getBTreeStorage() {
        return btreeStorage;
    }

    public PageStorageMode getPageStorageMode() {
        return pageStorageMode;
    }

    public void setPageStorageMode(PageStorageMode pageStorageMode) {
        this.pageStorageMode = pageStorageMode;
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

    // test only
    public int getLevel(K key) {
        int level = 1;
        Page p = root.gotoLeafPage(key);
        PageReference parentRef = p.getParentRef();
        while (parentRef != null) {
            level++;
            parentRef = parentRef.getPage().getParentRef();
        }
        return level;
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, boolean allColumns) {
        Page p = root.gotoLeafPage(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, allColumns) : null;
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, int[] columnIndexes) {
        Page p = root.gotoLeafPage(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, columnIndexes) : null;
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
    private K getFirstLast(boolean first) {
        if (isEmpty()) {
            return null;
        }
        Page p = root;
        while (true) {
            if (p.isLeaf()) {
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
    private K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(Page p, K key, boolean min, boolean excluding) {
        if (p.isLeaf()) {
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
        int x = p.getPageIndex(key);
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

    public void incrementSize() {
        size.incrementAndGet();
    }

    public void decrementSize() {
        size.decrementAndGet();
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
        return inMemory;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return cursor(CursorParameters.create(from));
    }

    @Override
    public StorageMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        if (parameters.pageKeys == null)
            return new BTreeCursor<>(this, parameters);
        else
            return new PageKeyCursor<>(this, parameters);
    }

    @Override
    public void clear() {
        checkWrite();
        try {
            acquireExclusiveLock();

            List<String> replicationHostIds = root.getReplicationHostIds();
            root.removeAllRecursive();
            size.set(0);
            maxKey.set(0);
            newRoot(LeafPage.createEmpty(this));
            root.setReplicationHostIds(replicationHostIds);
        } finally {
            releaseExclusiveLock();
        }
    }

    @Override
    public void remove() {
        try {
            acquireExclusiveLock();

            btreeStorage.remove();
            closeMap();
        } finally {
            releaseExclusiveLock();
        }
    }

    @Override
    public boolean isClosed() {
        return btreeStorage.isClosed();
    }

    @Override
    public void close() {
        try {
            acquireExclusiveLock();

            closeMap();
            btreeStorage.close();
        } finally {
            releaseExclusiveLock();
        }
    }

    private void closeMap() {
        storage.closeMap(name);
    }

    @Override
    public void save() {
        try {
            acquireSharedLock(); // 用共享锁

            btreeStorage.save();
        } finally {
            releaseSharedLock();
        }
    }

    public int getChildPageCount(Page p) {
        return p.getRawChildPageCount();
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
        return name;
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

    public Page gotoLeafPage(Object key) {
        return root.gotoLeafPage(key);
    }

    public Page gotoLeafPage(Object key, boolean markDirty) {
        return root.gotoLeafPage(key, markDirty);
    }

    // 如果map是只读的或者已经关闭了就不能再写了，并且不允许值为null
    private void checkWrite(V value) {
        DataUtils.checkNotNull(value, "value");
        checkWrite();
    }

    private void checkWrite() {
        if (btreeStorage.isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException("This map is read-only");
        }
    }

    //////////////////// 以下是同步和异步API的实现 ////////////////////////////////

    @Override
    public void get(K key, AsyncHandler<AsyncResult<V>> handler) {
        V v = get(key);
        handler.handle(new AsyncResult<>(v));
    }

    @Override
    public V put(K key, V value) {
        return put0(key, value, null);
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(key, value, handler);
    }

    private V put0(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        Put<K, V, V> put = new Put<>(this, key, value, handler);
        return runPageOperation(put);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent0(key, value, null);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(key, value, handler);
    }

    private V putIfAbsent0(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        PutIfAbsent<K, V> putIfAbsent = new PutIfAbsent<>(this, key, value, handler);
        return runPageOperation(putIfAbsent);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace0(key, oldValue, newValue, null);
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(key, oldValue, newValue, handler);
    }

    private boolean replace0(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        checkWrite(newValue);
        Replace<K, V> replace = new Replace<>(this, key, oldValue, newValue, handler);
        return runPageOperation(replace);
    }

    @Override
    public K append(V value) {
        return append0(value, null);
    }

    @Override
    public K append(V value, AsyncHandler<AsyncResult<K>> handler) {
        return append0(value, handler);
    }

    @SuppressWarnings("unchecked")
    private K append0(V value, AsyncHandler<AsyncResult<K>> handler) {
        checkWrite(value);
        // 先得到一个long类型的key
        K key = (K) ValueLong.get(maxKey.incrementAndGet());
        Append<K, V> append = new Append<>(this, key, value, handler);
        runPageOperation(append);
        return key;
    }

    @Override
    public V remove(K key) {
        return remove0(key, null);
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(key, handler);
    }

    private V remove0(K key, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite();
        Remove<K, V> remove = new Remove<>(this, key, handler);
        return runPageOperation(remove);
    }

    private <R> R runPageOperation(SingleWrite<?, ?, R> po) {
        PageOperationHandler poHandler = getPageOperationHandler(false);
        // 先快速试一次，如果不成功再用异步等待的方式
        if (po.run(poHandler) == PageOperationResult.SUCCEEDED)
            return po.getResult();
        poHandler = getPageOperationHandler(true);
        if (po.getResultHandler() == null) { // 同步
            PageOperation.Listener<R> listener = getPageOperationListener();
            po.setResultHandler(listener);
            poHandler.handlePageOperation(po);
            return listener.await();
        } else { // 异步
            poHandler.handlePageOperation(po);
            return null;
        }
    }

    // 如果当前线程不是PageOperationHandler，第一次运行时创建一个DummyPageOperationHandler
    // 第二次运行时需要加到现有线程池某个PageOperationHandler的队列中
    private PageOperationHandler getPageOperationHandler(boolean useThreadPool) {
        Object t = Thread.currentThread();
        if (t instanceof PageOperationHandler) {
            return (PageOperationHandler) t;
        } else {
            if (useThreadPool) {
                return pohFactory.getPageOperationHandler();
            } else {
                return new PageOperationHandler.DummyPageOperationHandler();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <R> PageOperation.Listener<R> getPageOperationListener() {
        Object object = Thread.currentThread();
        PageOperation.Listener<R> listener;
        if (object instanceof PageOperation.Listener)
            listener = (PageOperation.Listener<R>) object;
        else if (object instanceof PageOperation.ListenerFactory)
            listener = ((PageOperation.ListenerFactory<R>) object).createListener();
        else
            listener = new PageOperation.SyncListener<R>();
        listener.startListen();
        return listener;
    }
}
