/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.io.InputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DbSetting;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerListener;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.db.session.Session;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMapBase;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.aose.btree.chunk.ChunkManager;
import com.lealone.storage.aose.btree.page.LeafPage;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageReference;
import com.lealone.storage.aose.btree.page.PageStorageMode;
import com.lealone.storage.aose.btree.page.PageUtils;
import com.lealone.storage.aose.btree.page.PrettyPagePrinter;
import com.lealone.storage.aose.btree.page.PageOperations.Append;
import com.lealone.storage.aose.btree.page.PageOperations.Put;
import com.lealone.storage.aose.btree.page.PageOperations.PutIfAbsent;
import com.lealone.storage.aose.btree.page.PageOperations.Remove;
import com.lealone.storage.aose.btree.page.PageOperations.Replace;
import com.lealone.storage.aose.btree.page.PageOperations.WriteOperation;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.page.PageOperation.PageOperationResult;
import com.lealone.storage.type.StorageDataType;
import com.lealone.transaction.TransactionEngine;

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
    private final ReentrantLock lock = new ReentrantLock();

    private final boolean readOnly;
    private final boolean inMemory;
    private final Map<String, Object> config;
    private final BTreeStorage btreeStorage;
    private PageStorageMode pageStorageMode = PageStorageMode.ROW_STORAGE;

    private static class RootPageReference extends PageReference {

        public RootPageReference(BTreeStorage bs) {
            super(bs);
        }

        @Override
        public void replacePage(Page newRoot) {
            newRoot.setRef(this);
            if (getPage() != newRoot && newRoot.isNode()) {
                for (PageReference ref : newRoot.getChildren()) {
                    if (ref.getPage() != null && ref.getParentRef() != this)
                        ref.setParentRef(this);
                }
            }
            super.replacePage(newRoot);
        }

        @Override
        public boolean isRoot() {
            return true;
        }
    }

    // btree的root page引用，最开始是一个leaf page，随时都会指向新的page
    private final RootPageReference rootRef;
    private final SchedulerFactory schedulerFactory;

    public BTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, Object> config, AOStorage aoStorage) {
        super(name, keyType, valueType, aoStorage);
        DataUtils.checkNotNull(config, "config");
        schedulerFactory = aoStorage.getSchedulerFactory();
        // 只要包含就为true
        readOnly = config.containsKey(DbSetting.READ_ONLY.name());
        inMemory = config.containsKey(StorageSetting.IN_MEMORY.name());

        this.config = config;
        Object mode = config.get(StorageSetting.PAGE_STORAGE_MODE.name());
        if (mode != null) {
            pageStorageMode = PageStorageMode.valueOf(mode.toString().toUpperCase());
        }
        btreeStorage = new BTreeStorage(this);
        rootRef = new RootPageReference(btreeStorage);
        Chunk lastChunk = btreeStorage.getChunkManager().getLastChunk();
        if (lastChunk != null) {
            size.set(lastChunk.mapSize);
            rootRef.getPageInfo().pos = lastChunk.rootPagePos;
            Page root = rootRef.getOrReadPage();
            // 提前设置，如果root page是node类型，子page就能在Page.getChildPage中找到ParentRef
            rootRef.replacePage(root);
            setMaxKey(lastKey());
        } else {
            Page root = LeafPage.createEmpty(this);
            rootRef.replacePage(root);
        }
    }

    public Page getRootPage() {
        return rootRef.getOrReadPage();
    }

    public PageReference getRootPageRef() {
        return rootRef;
    }

    public void newRoot(Page newRoot) {
        rootRef.replacePage(newRoot);
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

    public SchedulerFactory getSchedulerFactory() {
        return schedulerFactory;
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
    public Object[] getObjects(K key, int[] columnIndexes) {
        Page p = getRootPage().gotoLeafPage(key);
        int index = p.binarySearch(key);
        Object v = index >= 0 ? p.getValue(index, columnIndexes) : null;
        return new Object[] { p, v };
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, boolean allColumns) {
        Page p = getRootPage().gotoLeafPage(key);
        int index = p.binarySearch(key);
        return index >= 0 ? (V) p.getValue(index, allColumns) : null;
    }

    @SuppressWarnings("unchecked")
    private V binarySearch(Object key, int[] columnIndexes) {
        Page p = getRootPage().gotoLeafPage(key);
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
        Page p = getRootPage();
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
        return getMinMax(getRootPage(), key, min, excluding);
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

    @Override
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
    public StorageMapCursor<K, V> cursor(CursorParameters<K> parameters) {
        return new BTreeCursor<>(this, parameters);
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            checkWrite();
            btreeStorage.clear();
            size.set(0);
            maxKey.set(0);
            newRoot(LeafPage.createEmpty(this));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void remove() {
        lock.lock();
        try {
            clear(); // 及早释放内存，上层的数据库对象模型可能会引用到，容易产生OOM
            btreeStorage.remove();
            closeMap();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return btreeStorage.isClosed();
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closeMap();
            btreeStorage.close();
        } finally {
            lock.unlock();
        }
    }

    private void closeMap() {
        storage.closeMap(name);
    }

    @Override
    public void save() {
        if (!inMemory) {
            lock.lock();
            try {
                btreeStorage.save();
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void save(long dirtyMemory) {
        if (!inMemory) {
            lock.lock();
            try {
                btreeStorage.save((int) dirtyMemory);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public boolean needGc() {
        return !inMemory && btreeStorage.getBTreeGC().needGc();
    }

    @Override
    public void gc(TransactionEngine te) {
        if (!inMemory) {
            lock.lock();
            try {
                btreeStorage.getBTreeGC().gc(te);
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public void fullGc(TransactionEngine te) {
        if (!inMemory) {
            // 如果加锁失败可以直接返回
            if (lock.tryLock()) {
                try {
                    btreeStorage.save(false, (int) collectDirtyMemory(te, null));
                    btreeStorage.getBTreeGC().fullGc(te);
                } finally {
                    lock.unlock();
                }
            }
        }
    }

    @Override
    public long collectDirtyMemory(TransactionEngine te, AtomicLong usedMemory) {
        lock.lock();
        try {
            return inMemory ? 0 : btreeStorage.getBTreeGC().collectDirtyMemory(te, usedMemory);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void markDirty(Object key) {
        gotoLeafPage(key).markDirtyBottomUp();
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
        PrettyPagePrinter.printPage(getRootPage(), readOffLinePage);
    }

    @Override
    public long getDiskSpaceUsed() {
        return btreeStorage.getDiskSpaceUsed();
    }

    @Override
    public long getMemorySpaceUsed() {
        return btreeStorage.getMemorySpaceUsed();
    }

    @Override
    public boolean hasUnsavedChanges() {
        return getRootPage().getPos() == 0;
    }

    public Page gotoLeafPage(Object key) {
        return getRootPage().gotoLeafPage(key);
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

    @Override
    @SuppressWarnings("unchecked")
    public void repair() {
        lock.lock();
        try {
            ChunkManager chunkManager = btreeStorage.getChunkManager();
            HashSet<Long> removedPages = new HashSet<>();
            HashSet<Long> pages = new HashSet<>();
            for (Integer id : chunkManager.getAllChunkIds()) {
                Chunk c = chunkManager.getChunk(id);
                removedPages.addAll(c.getRemovedPages());
                pages.addAll(c.pagePositionToLengthMap.keySet());
            }
            clear();
            pages.removeAll(removedPages);
            for (Long p : pages) {
                if (PageUtils.isNodePage(p))
                    continue;
                PageReference tmpRef = new PageReference(btreeStorage, p);
                Page leaf = tmpRef.getOrReadPage();
                int keys = leaf.getKeyCount();
                for (int i = 0; i < keys; i++) {
                    put((K) leaf.getKey(i), (V) leaf.getValue(i));
                }
            }
            btreeStorage.save();
        } finally {
            lock.unlock();
        }
    }

    //////////////////// 以下是同步和异步API的实现 ////////////////////////////////

    @Override
    public V put(K key, V value) {
        return put0(null, key, value, null);
    }

    @Override
    public void put(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(null, key, value, handler);
    }

    @Override
    public void put(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        put0(session, key, value, handler);
    }

    private V put0(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        Put<K, V, V> put = new Put<>(this, key, value, handler);
        return runPageOperation(session, put);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent0(null, key, value, null);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(null, key, value, handler);
    }

    @Override
    public void putIfAbsent(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        putIfAbsent0(session, key, value, handler);
    }

    private V putIfAbsent0(Session session, K key, V value, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite(value);
        PutIfAbsent<K, V> putIfAbsent = new PutIfAbsent<>(this, key, value, handler);
        return runPageOperation(session, putIfAbsent);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return replace0(null, key, oldValue, newValue, null);
    }

    @Override
    public void replace(K key, V oldValue, V newValue, AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(null, key, oldValue, newValue, handler);
    }

    @Override
    public void replace(Session session, K key, V oldValue, V newValue,
            AsyncHandler<AsyncResult<Boolean>> handler) {
        replace0(session, key, oldValue, newValue, handler);
    }

    private Boolean replace0(Session session, K key, V oldValue, V newValue,
            AsyncHandler<AsyncResult<Boolean>> handler) {
        checkWrite(newValue);
        Replace<K, V> replace = new Replace<>(this, key, oldValue, newValue, handler);
        return runPageOperation(session, replace);
    }

    @Override
    public K append(V value) {
        return append0(null, value, null);
    }

    @Override
    public void append(V value, AsyncHandler<AsyncResult<K>> handler) {
        append0(null, value, handler);
    }

    @Override
    public void append(Session session, V value, AsyncHandler<AsyncResult<K>> handler) {
        append0(session, value, handler);
    }

    private K append0(Session session, V value, AsyncHandler<AsyncResult<K>> handler) {
        checkWrite(value);
        Append<K, V> append = new Append<>(this, value, handler);
        return runPageOperation(session, append);
    }

    @Override
    public V remove(K key) {
        return remove0(null, key, null);
    }

    @Override
    public void remove(K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(null, key, handler);
    }

    @Override
    public void remove(Session session, K key, AsyncHandler<AsyncResult<V>> handler) {
        remove0(session, key, handler);
    }

    private V remove0(Session session, K key, AsyncHandler<AsyncResult<V>> handler) {
        checkWrite();
        Remove<K, V> remove = new Remove<>(this, key, handler);
        return runPageOperation(session, remove);
    }

    private <R> R runPageOperation(Session session, WriteOperation<?, ?, R> po) {
        Scheduler scheduler;
        if (session != null && session.getScheduler() != null) {
            po.setSession(session);
            scheduler = session.getScheduler();
        } else {
            scheduler = SchedulerThread.currentScheduler(schedulerFactory);
            if (scheduler == null) {
                // 如果不是调度线程且现有的调度线程都绑定完了，需要委派给一个调度线程去执行
                scheduler = schedulerFactory.getScheduler();
                return handlePageOperation(scheduler, po);
            }
        }
        // 第一步: 先快速试3次，如果不成功再转到第二步
        int maxRetryCount = 3;
        while (true) {
            PageOperationResult result = po.run(scheduler, maxRetryCount == 1);
            if (result == PageOperationResult.SUCCEEDED)
                return po.getResult();
            else if (result == PageOperationResult.LOCKED) {
                --maxRetryCount;
            } else if (result == PageOperationResult.RETRY) {
                continue;
            }
            if (maxRetryCount < 1)
                break;
        }
        // 第二步:
        // 其他PageOperationHandler占用了锁时，当前PageOperationHandler把PageOperation放到自己的等待队列。
        // 如果当前线程(也就是PageOperationHandler)执行的是异步调用那就直接返回，否则需要等待。
        return handlePageOperation(scheduler, po);
    }

    private <R> R handlePageOperation(Scheduler scheduler, WriteOperation<?, ?, R> po) {
        if (po.getResultHandler() == null) { // 同步
            SchedulerListener<R> listener = SchedulerListener.createSchedulerListener();
            po.setResultHandler(listener);
            scheduler.handlePageOperation(po);
            return listener.await();
        } else { // 异步
            scheduler.handlePageOperation(po);
            return null;
        }
    }

    public InputStream getInputStream(FilePath file) {
        return btreeStorage.getInputStream(file);
    }
}
