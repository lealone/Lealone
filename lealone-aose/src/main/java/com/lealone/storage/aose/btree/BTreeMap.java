/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DbSetting;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.scheduler.SchedulerListener;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.db.session.InternalSession;
import com.lealone.storage.CursorParameters;
import com.lealone.storage.StorageMapBase;
import com.lealone.storage.StorageMapCursor;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.aose.btree.chunk.ChunkManager;
import com.lealone.storage.aose.btree.page.LeafPage;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageOperations.Append;
import com.lealone.storage.aose.btree.page.PageOperations.Put;
import com.lealone.storage.aose.btree.page.PageOperations.PutIfAbsent;
import com.lealone.storage.aose.btree.page.PageOperations.Remove;
import com.lealone.storage.aose.btree.page.PageOperations.WriteOperation;
import com.lealone.storage.aose.btree.page.PageReference;
import com.lealone.storage.aose.btree.page.PageStorageMode;
import com.lealone.storage.aose.btree.page.PageUtils;
import com.lealone.storage.aose.btree.page.PrettyPagePrinter;
import com.lealone.storage.page.PageOperation.PageOperationResult;
import com.lealone.storage.type.StorageDataType;

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

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    // 执行save、垃圾收集、读写RedoLog时用共享锁，它们之间再用RedoLog的锁
    private final ReentrantReadWriteLock.ReadLock sharedLock = rwLock.readLock();
    // 执行clear、remove、close、repair用排他锁
    private final ReentrantReadWriteLock.WriteLock exclusiveLock = rwLock.writeLock();

    private final boolean readOnly;
    private final boolean inMemory;
    private final Map<String, Object> config;
    private final BTreeStorage btreeStorage;
    private final PageStorageMode pageStorageMode;

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
        } else {
            pageStorageMode = PageStorageMode.ROW_STORAGE;
        }
        btreeStorage = new BTreeStorage(this);
        rootRef = new RootPageReference(btreeStorage);
        Chunk lastChunk = btreeStorage.getChunkManager().getLastChunk();
        if (lastChunk != null && lastChunk.rootPagePos != 0) {
            size.set(lastChunk.mapSize);
            rootRef.getPageInfo().pos = lastChunk.rootPagePos;
            Page root = rootRef.getOrReadPage();
            // 提前设置，如果root page是node类型，子page就能在Page.getChildPage中找到ParentRef
            rootRef.replacePage(root);
            if (lastChunk.mapMaxKey != null)
                setMaxKey(lastChunk.mapMaxKey);
            else
                setMaxKey(lastKey()); // lealone 6.1.0之前的版本会读取最后一个page,表多时启动数据库会稍微慢一点点
        } else {
            Page root = createEmptyPage();
            rootRef.replacePage(root);
        }
    }

    private Page createEmptyPage() {
        return createEmptyPage(true);
    }

    public Page createEmptyPage(boolean addToUsedMemory) {
        return LeafPage.createEmpty(this, addToUsedMemory);
    }

    public Page getRootPage() {
        return rootRef.getOrReadPage();
    }

    public PageReference getRootPageRef() {
        return rootRef;
    }

    public void newRoot(Page newRoot) {
        // 变更PageLock，让老的记录重新定位
        rootRef.setNewPageLock();
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
    @SuppressWarnings("unchecked")
    public V get(K key, int[] columnIndexes) {
        Page p = getRootPage().gotoLeafPage(key);
        int index = p.binarySearch(key);
        Object v = index >= 0 ? p.getValue(index, columnIndexes) : null;
        return (V) v;
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
        exclusiveLock.lock();
        try {
            checkWrite();
            rootRef.markDirtyPage();
            btreeStorage.clear();
            size.set(0);
            maxKey.set(0);
            newRoot(createEmptyPage());
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public void remove() {
        exclusiveLock.lock();
        try {
            clear(); // 及早释放内存，上层的数据库对象模型可能会引用到，容易产生OOM
            btreeStorage.remove();
            closeMap();
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return btreeStorage.isClosed();
    }

    @Override
    public void close() {
        exclusiveLock.lock();
        try {
            closeMap();
            btreeStorage.close();
        } finally {
            exclusiveLock.unlock();
        }
    }

    private void closeMap() {
        storage.closeMap(name);
    }

    @Override
    public void save() {
        save(collectDirtyMemory());
    }

    @Override
    public void save(long dirtyMemory) {
        save(true, false, dirtyMemory);
    }

    public void save(boolean appendModeEnabled) {
        save(true, appendModeEnabled, collectDirtyMemory());
    }

    public void save(boolean compact, boolean appendModeEnabled, long dirtyMemory) {
        if (!inMemory) {
            sharedLock.lock();
            try {
                btreeStorage.save(compact, appendModeEnabled, dirtyMemory);
            } finally {
                sharedLock.unlock();
            }
        }
    }

    @Override
    public void gc() {
        if (!inMemory && sharedLock.tryLock()) { // 如果加锁失败可以直接返回
            try {
                btreeStorage.getBTreeGC().gc();
            } finally {
                sharedLock.unlock();
            }
        }
    }

    @Override
    public void fullGc() {
        if (!inMemory && sharedLock.tryLock()) { // 如果加锁失败可以直接返回
            try {
                btreeStorage.getBTreeGC().fullGc();
            } finally {
                sharedLock.unlock();
            }
        }
    }

    @Override
    public long collectDirtyMemory() {
        if (inMemory)
            return 0;
        sharedLock.lock();
        try {
            return btreeStorage.getBTreeGC().collectDirtyMemory();
        } finally {
            sharedLock.unlock();
        }
    }

    @Override
    public int getCacheSize() {
        return btreeStorage.getCacheSize();
    }

    public void markDirty(Object key) {
        gotoLeafPage(key).getRef().markDirtyPage();
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
        if (inMemory)
            return;
        exclusiveLock.lock();
        try {
            ChunkManager chunkManager = btreeStorage.getChunkManager();
            HashSet<Long> pages = new HashSet<>();
            HashSet<Long> removedPages = chunkManager.getAllRemovedPages();
            ArrayList<Chunk> oldChunks = new ArrayList<>();
            for (Integer id : chunkManager.getAllChunkIds()) {
                Chunk c = chunkManager.getChunk(id);
                oldChunks.add(c);
                removedPages.addAll(c.getRemovedPages());
                pages.addAll(c.pagePositionToLengthMap.keySet());
            }
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
            for (Chunk c : oldChunks) {
                chunkManager.removeUnusedChunk(c);
            }
        } finally {
            exclusiveLock.unlock();
        }
    }

    //////////////////// 以下是同步和异步API的实现 ////////////////////////////////

    @Override
    public V put(K key, V value) {
        return put0(null, key, value, null);
    }

    @Override
    public void put(K key, V value, AsyncResultHandler<V> handler) {
        put0(null, key, value, handler);
    }

    @Override
    public void put(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        put0(session, key, value, handler);
    }

    private V put0(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        checkWrite(value);
        Put<K, V, V> put = new Put<>(this, key, value, handler);
        return runPageOperation(session, put);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return putIfAbsent0(null, key, value, null);
    }

    @Override
    public void putIfAbsent(K key, V value, AsyncResultHandler<V> handler) {
        putIfAbsent0(null, key, value, handler);
    }

    @Override
    public void putIfAbsent(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        putIfAbsent0(session, key, value, handler);
    }

    private V putIfAbsent0(InternalSession session, K key, V value, AsyncResultHandler<V> handler) {
        checkWrite(value);
        PutIfAbsent<K, V> putIfAbsent = new PutIfAbsent<>(this, key, value, handler);
        return runPageOperation(session, putIfAbsent);
    }

    @Override
    public K append(V value) {
        return append0(null, value, null);
    }

    @Override
    public void append(V value, AsyncResultHandler<K> handler) {
        append0(null, value, handler);
    }

    @Override
    public void append(InternalSession session, V value, AsyncResultHandler<K> handler) {
        append0(session, value, handler);
    }

    private K append0(InternalSession session, V value, AsyncResultHandler<K> handler) {
        checkWrite(value);
        Append<K, V> append = new Append<>(this, value, handler);
        return runPageOperation(session, append);
    }

    @SuppressWarnings("unchecked")
    public void remove(PageReference ref, Object key) {
        if (ref.isDataStructureChanged()) {
            remove0(null, (K) key, null);
        } else {
            checkWrite();
            Remove<K, V> remove = new Remove<>(this, (K) key, null);
            remove.setPageReference(ref);
            runPageOperation(null, remove);
        }
    }

    @Override
    public V remove(K key) {
        return remove0(null, key, null);
    }

    @Override
    public void remove(K key, AsyncResultHandler<V> handler) {
        remove0(null, key, handler);
    }

    @Override
    public void remove(InternalSession session, K key, AsyncResultHandler<V> handler) {
        remove0(session, key, handler);
    }

    private V remove0(InternalSession session, K key, AsyncResultHandler<V> handler) {
        checkWrite();
        Remove<K, V> remove = new Remove<>(this, key, handler);
        return runPageOperation(session, remove);
    }

    private <R> R runPageOperation(InternalSession session, WriteOperation<?, ?, R> po) {
        InternalScheduler scheduler;
        if (session != null && session.getScheduler() != null) {
            po.setSession(session);
            scheduler = session.getScheduler();
        } else {
            scheduler = (InternalScheduler) SchedulerThread.bindScheduler(schedulerFactory);
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
            } else if (result == PageOperationResult.FAILED) {
                return null;
            }
            if (maxRetryCount < 1)
                break;
        }
        // 第二步:
        // 其他PageOperationHandler占用了锁时，当前PageOperationHandler把PageOperation放到自己的等待队列。
        // 如果当前线程(也就是PageOperationHandler)执行的是异步调用那就直接返回，否则需要等待。
        return handlePageOperation(scheduler, po);
    }

    private <R> R handlePageOperation(InternalScheduler scheduler, WriteOperation<?, ?, R> po) {
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

    @Override
    public void writeRedoLog(ByteBuffer log) {
        sharedLock.lock();
        try {
            if (!isClosed())
                btreeStorage.writeRedoLog(log);
        } finally {
            sharedLock.unlock();
        }
    }

    @Override
    public ByteBuffer readRedoLog() {
        sharedLock.lock();
        try {
            return btreeStorage.readRedoLog();
        } finally {
            sharedLock.unlock();
        }
    }

    @Override
    public void sync() {
        sharedLock.lock();
        try {
            if (!isClosed())
                btreeStorage.sync();
        } finally {
            sharedLock.unlock();
        }
    }

    private int redoLogServiceIndex = -1;

    @Override
    public int getRedoLogServiceIndex() {
        return redoLogServiceIndex;
    }

    @Override
    public void setRedoLogServiceIndex(int index) {
        redoLogServiceIndex = index;
    }

    private RedoLogBuffer redoLogBuffer;

    @Override
    public RedoLogBuffer getRedoLogBuffer() {
        return redoLogBuffer;
    }

    @Override
    public void setRedoLogBuffer(RedoLogBuffer redoLogBuffer) {
        this.redoLogBuffer = redoLogBuffer;
    }

    private long lastTransactionId = -1;

    @Override
    public long getLastTransactionId() {
        return lastTransactionId;
    }

    @Override
    public void setLastTransactionId(long lastTransactionId) {
        this.lastTransactionId = lastTransactionId;
    }

    @Override
    public boolean validateRedoLog(long lastTransactionId) {
        return btreeStorage.validateRedoLog(lastTransactionId);
    }
}
