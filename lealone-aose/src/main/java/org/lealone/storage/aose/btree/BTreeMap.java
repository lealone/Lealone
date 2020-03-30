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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.Future;
import org.lealone.db.session.Session;
import org.lealone.db.value.ValueLong;
import org.lealone.net.NetNode;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.PageOperations.Get;
import org.lealone.storage.aose.btree.PageOperations.Put;
import org.lealone.storage.aose.btree.PageOperations.PutIfAbsent;
import org.lealone.storage.aose.btree.PageOperations.Remove;
import org.lealone.storage.aose.btree.PageOperations.Replace;
import org.lealone.storage.aose.btree.PageOperations.RunnableOperation;
import org.lealone.storage.replication.ReplicationSession;
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
        if (config.containsKey("isShardingMode"))
            isShardingMode = Boolean.parseBoolean(config.get("isShardingMode").toString());
        else
            isShardingMode = false;
        db = (IDatabase) config.get("db");

        btreeStorage = new BTreeStorage((BTreeMap<Object, Object>) this);

        if (btreeStorage.lastChunk != null) {
            root = btreeStorage.readPage(btreeStorage.lastChunk.rootPagePos);
            setMaxKey(lastKey());
            size.set(btreeStorage.lastChunk.mapSize);
        } else {
            if (isShardingMode) {
                String initReplicationNodes = (String) config.get("initReplicationNodes");
                DataUtils.checkArgument(initReplicationNodes != null, "The initReplicationNodes may not be null");
                String[] replicationNodes = StringUtils.arraySplit(initReplicationNodes, '&');
                if (containsLocalNode(replicationNodes)) {
                    root = BTreeLeafPage.createEmpty(this);
                } else {
                    root = new BTreeRemotePage(this);
                }
                root.setReplicationHostIds(Arrays.asList(replicationNodes));
                btreeStorage.addHostIds(replicationNodes);
                // 强制把replicationHostIds持久化
                btreeStorage.forceSave();
            } else {
                root = BTreeLeafPage.createEmpty(this);
            }
        }
        disableParallelIfNeeded();
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

    ////////////////////// 以下是分布式API的实现 ////////////////////////////////

    protected boolean isShardingMode;
    protected IDatabase db;
    private String[] oldNodes;

    private boolean containsLocalNode(String[] replicationNodes) {
        NetNode local = NetNode.getLocalTcpNode();
        for (String e : replicationNodes) {
            if (local.equals(NetNode.createTCP(e)))
                return true;
        }
        return false;
    }

    protected IDatabase getDatabase() {
        return db;
    }

    protected void fireLeafPageSplit(Object splitKey) {
        if (isShardingMode()) {
            PageKey pk = new PageKey(splitKey, false); // 移动右边的Page
            moveLeafPageLazy(pk);
        }
    }

    private String getLocalHostId() {
        return db.getLocalHostId();
    }

    protected <R> Object removeRemote(BTreePage p, K key, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
        return null;
    }

    protected <R> Object putRemote(BTreePage p, K key, V value, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
        if (p.getLeafPageMovePlan().moverHostId.equals(getLocalHostId())) {
            int size = p.getLeafPageMovePlan().replicationNodes.size();
            List<NetNode> replicationNodes = new ArrayList<>(size);
            replicationNodes.addAll(p.getLeafPageMovePlan().replicationNodes);
            boolean containsLocalNode = replicationNodes.remove(getLocalNode());
            Object returnValue = null;
            ReplicationSession rs = db.createReplicationSession(db.createInternalSession(), replicationNodes);
            try (DataBuffer k = DataBuffer.create();
                    DataBuffer v = DataBuffer.create();
                    StorageCommand c = rs.createStorageCommand()) {
                ByteBuffer keyBuffer = k.write(keyType, key);
                ByteBuffer valueBuffer = v.write(valueType, value);
                byte[] oldValue = (byte[]) c.put(getName(), keyBuffer, valueBuffer, true).get();
                if (oldValue != null) {
                    returnValue = valueType.read(ByteBuffer.wrap(oldValue));
                }
            }
            // 如果新的复制节点中还包含本地节点，那么还需要put到本地节点中
            if (containsLocalNode) {
                return put(key, value); // TODO 可能还有bug
            } else {
                return returnValue;
            }
        } else {
            return null; // 不是由当前节点移动的，那么put操作就可以忽略了
        }
    }

    private Set<NetNode> getCandidateNodes() {
        return getCandidateNodes(db, db.getHostIds());
    }

    static Set<NetNode> getCandidateNodes(IDatabase db, String[] hostIds) {
        Set<NetNode> candidateNodes = new HashSet<>(hostIds.length);
        for (String hostId : hostIds) {
            candidateNodes.add(db.getNode(hostId));
        }
        return candidateNodes;
    }

    // 必需同步
    private synchronized BTreePage setLeafPageMovePlan(PageKey pageKey, LeafPageMovePlan leafPageMovePlan) {
        BTreePage page = root.binarySearchLeafPage(pageKey.key);
        if (page != null) {
            page.setLeafPageMovePlan(leafPageMovePlan);
        }
        return page;
    }

    private void moveLeafPageLazy(PageKey pageKey) {
        RunnableOperation operation = new RunnableOperation(() -> {
            moveLeafPage(pageKey);
        });
        pohFactory.addPageOperation(operation);
    }

    private void moveLeafPage(PageKey pageKey) {
        BTreePage p = root;
        BTreePage parent = p;
        int index = 0;
        while (p.isNode()) {
            index = p.binarySearch(pageKey.key);
            if (index < 0) {
                index = -index - 1;
                if (p.isRemoteChildPage(index))
                    return;
                parent = p;
                p = p.getChildPage(index);
            } else {
                index++;
                if (parent.isRemoteChildPage(index))
                    return;
                // 左边已经移动过了，那么右边就不需要再移
                if (parent.isRemoteChildPage(index - 1))
                    return;

                p = parent.getChildPage(index);
                String[] oldNodes;
                if (p.getReplicationHostIds() == null) {
                    oldNodes = new String[0];
                } else {
                    oldNodes = new String[p.getReplicationHostIds().size()];
                    p.getReplicationHostIds().toArray(oldNodes);
                }
                replicateOrMovePage(pageKey, p, parent, index, oldNodes, false);
                break;
            }
        }
    }

    // 处理三种场景:
    // 1. 从client_server模式转到sharding模式
    // 2. 从replication模式转到sharding模式
    // 3. 在sharding模式下发生page split时需要移动右边的page
    //
    // 前两种场景在移动page时所选定的目标节点可以是原来的节点，后一种不可以。
    // 除此之外，这三者并没有多大差异，只是oldNodes中包含的节点个数多少的问题，
    // client_server模式只有一个节点，在replication模式下，如果副本个数是1，那么也相当于client_server模式。
    private void replicateOrMovePage(PageKey pageKey, BTreePage p, BTreePage parent, int index, String[] oldNodes,
            boolean replicate) {
        Set<NetNode> candidateNodes = getCandidateNodes();
        replicateOrMovePage(pageKey, p, parent, index, oldNodes, replicate, candidateNodes);
    }

    void replicateOrMovePage(PageKey pageKey, BTreePage p, BTreePage parent, int index, String[] oldNodes,
            boolean replicate, Set<NetNode> candidateNodes) {
        if (oldNodes == null || oldNodes.length == 0) {
            DbException.throwInternalError("oldNodes is null");
        }

        List<NetNode> oldReplicationNodes = getReplicationNodes(db, oldNodes);
        Set<NetNode> oldNodeSet;
        if (replicate) {
            // 允许选择原来的节点，所以用new HashSet<>(0)替代new HashSet<>(oldReplicationNodes)
            oldNodeSet = new HashSet<>(0);
        } else {
            oldNodeSet = new HashSet<>(oldReplicationNodes);
        }

        List<NetNode> newReplicationNodes = db.getReplicationNodes(oldNodeSet, candidateNodes);

        Session session = db.createInternalSession();
        LeafPageMovePlan leafPageMovePlan = null;

        if (oldNodes.length == 1) {
            leafPageMovePlan = new LeafPageMovePlan(oldNodes[0], newReplicationNodes, pageKey);
            p.setLeafPageMovePlan(leafPageMovePlan);
        } else {
            ReplicationSession rs = db.createReplicationSession(session, oldReplicationNodes);
            try (StorageCommand c = rs.createStorageCommand()) {
                LeafPageMovePlan plan = new LeafPageMovePlan(getLocalHostId(), newReplicationNodes, pageKey);
                leafPageMovePlan = c.prepareMoveLeafPage(getName(), plan).get();
            }

            if (leafPageMovePlan == null)
                return;

            // 重新按key找到page，因为经过前面的操作后，
            // 可能page已经有新数据了，如果只移动老的，会丢失数据
            p = setLeafPageMovePlan(pageKey, leafPageMovePlan);

            if (!leafPageMovePlan.moverHostId.equals(getLocalHostId())) {
                p.setReplicationHostIds(leafPageMovePlan.getReplicationNodes());
                return;
            }
        }

        p.setReplicationHostIds(toHostIds(db, newReplicationNodes));
        NetNode localNode = getLocalNode();

        Set<NetNode> otherNodes = new HashSet<>(candidateNodes);
        otherNodes.removeAll(newReplicationNodes);

        if (parent != null && !replicate && !newReplicationNodes.contains(localNode)) {
            PageReference r = PageReference.createRemotePageReference(pageKey.key, index == 0);
            r.replicationHostIds = p.getReplicationHostIds();
            parent.setChild(index, r);
        }
        if (!replicate) {
            otherNodes.removeAll(oldReplicationNodes);
            newReplicationNodes.removeAll(oldReplicationNodes);
        }

        if (newReplicationNodes.contains(localNode)) {
            newReplicationNodes.remove(localNode);
        }

        // 移动page到新的复制节点(page中包含数据)
        if (!newReplicationNodes.isEmpty()) {
            ReplicationSession rs = db.createReplicationSession(session, newReplicationNodes, true);
            moveLeafPage(leafPageMovePlan.pageKey, p, rs, false, !replicate);
        }

        // 当前节点已经不是副本所在节点
        if (parent != null && replicate && otherNodes.contains(localNode)) {
            otherNodes.remove(localNode);
            PageReference r = PageReference.createRemotePageReference(pageKey.key, index == 0);
            r.replicationHostIds = p.getReplicationHostIds();
            parent.setChild(index, r);
        }

        // 移动page到其他节点(page中不包含数据，只包含这个page各数据副本所在节点信息)
        if (!otherNodes.isEmpty()) {
            ReplicationSession rs = db.createReplicationSession(session, otherNodes, true);
            moveLeafPage(leafPageMovePlan.pageKey, p, rs, true, !replicate);
        }
    }

    private void moveLeafPage(PageKey pageKey, BTreePage page, ReplicationSession rs, boolean remote, boolean addPage) {
        try (DataBuffer buff = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            page.writeLeaf(buff, remote);
            ByteBuffer pageBuffer = buff.getAndFlipBuffer();
            c.moveLeafPage(getName(), pageKey, pageBuffer, addPage);
        }
    }

    protected void fireLeafPageRemove(PageKey pageKey, BTreePage leafPage) {
        if (isShardingMode()) {
            removeLeafPage(pageKey, leafPage);
        }
    }

    private void removeLeafPage(PageKey pageKey, BTreePage leafPage) {
        if (leafPage.getReplicationHostIds().get(0).equals(getLocalHostId())) {
            RunnableOperation operation = new RunnableOperation(() -> {
                List<NetNode> oldReplicationNodes = getReplicationNodes(leafPage);
                Set<NetNode> otherNodes = getCandidateNodes();
                otherNodes.removeAll(oldReplicationNodes);
                Session session = db.createInternalSession();
                ReplicationSession rs = db.createReplicationSession(session, otherNodes, true);
                try (StorageCommand c = rs.createStorageCommand()) {
                    c.removeLeafPage(getName(), pageKey);
                }
            });
            pohFactory.addPageOperation(operation);
        }
    }

    @Override
    public synchronized void addLeafPage(PageKey pageKey, ByteBuffer page, boolean addPage) {
        addLeafPage(pageKey, page, addPage, false);
    }

    private BTreePage readStreamPage(ByteBuffer buff) {
        BTreePage p = new BTreeLeafPage(this);
        int chunkId = 0;
        int offset = buff.position();
        p.read(buff, chunkId, offset, buff.limit(), true);
        return p;
    }

    private BTreePage readLeafPage(ByteBuffer buff, boolean readStreamPage) {
        return readStreamPage ? readStreamPage(buff) : BTreePage.readLeafPage(this, buff);
    }

    private synchronized void addLeafPage(PageKey pageKey, ByteBuffer page, boolean addPage, boolean readStreamPage) {
        if (pageKey == null) {
            root = readLeafPage(page, readStreamPage);
            return;
        }
        BTreePage p = root;
        Object k = pageKey.key;
        if (p.isLeaf()) {
            Object[] keys = { k };
            BTreePage left = BTreeLeafPage.createEmpty(this);
            left.setReplicationHostIds(p.getReplicationHostIds());

            BTreePage right = readLeafPage(page, readStreamPage);

            PageReference[] children = { new PageReference(left, k, true), new PageReference(right, k, false) };
            p = BTreePage.createNode(this, keys, children, 0);
            newRoot(p);
        } else {
            BTreePage parent = p;
            int index = 0;
            while (p.isNode()) {
                parent = p;
                index = p.binarySearch(k);
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                PageReference r = p.getChildPageReference(index);
                if (r.isRemotePage()) {
                    break;
                }
                p = p.getChildPage(index);
            }
            BTreePage right = readLeafPage(page, readStreamPage);
            if (addPage) {
                BTreePage left = parent.getChildPage(index);
                parent.setChild(index, right);
                parent.insertNode(index, k, left);
            } else {
                parent.setChild(index, right);
            }
        }
    }

    @Override
    public synchronized void removeLeafPage(PageKey pageKey) {
        checkWrite();
        BTreePage p;
        if (pageKey == null) { // 说明删除的是root leaf page
            p = BTreeLeafPage.createEmpty(this);
        } else {
            p = root.copy();
            removeLeafPage(p, pageKey);
            if (p.isNode() && p.isEmpty()) {
                p.removePage();
                p = BTreeLeafPage.createEmpty(this);
            }
        }
        newRoot(p);
    }

    private void removeLeafPage(BTreePage p, PageKey pk) {
        if (p.isLeaf()) {
            return;
        }
        // node page
        int x = p.binarySearch(pk.key);
        if (x < 0) {
            x = -x - 1;
        } else {
            x++;
        }
        if (pk.first && p.isLeafChildPage(x)) {
            x = 0;
        }
        BTreePage cOld = p.getChildPage(x);
        BTreePage c = cOld.copy();
        removeLeafPage(c, pk);
        if (c.isLeaf())
            c.removePage();
        else
            p.setChild(x, c);
        if (p.getKeyCount() == 0) { // 如果p的子节点只剩一个叶子节点时，keyCount为0
            p.setChild(x, (BTreePage) null);
        } else {
            if (c.isLeaf())
                p.remove(x);
        }
    }

    @Override
    public List<NetNode> getReplicationNodes(Object key) {
        return getReplicationNodes(root, key);
    }

    private List<NetNode> getReplicationNodes(BTreePage p, Object key) {
        if (p.isLeaf() || p.isRemote()) {
            return getReplicationNodes(p);
        }
        int index = p.binarySearch(key);
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        return getReplicationNodes(p.getChildPage(index), key);
    }

    private List<NetNode> getReplicationNodes(BTreePage p) {
        return getReplicationNodes(db, p.getReplicationHostIds());
    }

    public static List<NetNode> getReplicationNodes(IDatabase db, String[] replicationHostIds) {
        return getReplicationNodes(db, Arrays.asList(replicationHostIds));
    }

    static List<NetNode> getReplicationNodes(IDatabase db, List<String> replicationHostIds) {
        int size = replicationHostIds.size();
        List<NetNode> replicationNodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationNodes.add(db.getNode(replicationHostIds.get(i)));
        }
        return replicationNodes;
    }

    private List<NetNode> getLastPageReplicationNodes() {
        BTreePage p = root;
        while (true) {
            if (p.isLeaf()) {
                return getReplicationNodes(p);
            }
            p = p.getChildPage(getChildPageCount(p) - 1);
        }
    }

    NetNode getLocalNode() {
        return NetNode.getLocalP2pNode();
    }

    @Override
    public Object replicationPut(Session session, Object key, Object value, StorageDataType valueType) {
        List<NetNode> replicationNodes = getReplicationNodes(key);
        ReplicationSession rs = db.createReplicationSession(session, replicationNodes);
        try (DataBuffer k = DataBuffer.create();
                DataBuffer v = DataBuffer.create();
                StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer keyBuffer = k.write(keyType, key);
            ByteBuffer valueBuffer = v.write(valueType, value);
            byte[] oldValue = (byte[]) c.put(getName(), keyBuffer, valueBuffer, false).get();
            if (oldValue == null)
                return null;
            return valueType.read(ByteBuffer.wrap(oldValue));
        }
    }

    @Override
    public Object replicationGet(Session session, Object key) {
        List<NetNode> replicationNodes = getReplicationNodes(key);
        ReplicationSession rs = db.createReplicationSession(session, replicationNodes);
        try (DataBuffer k = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer keyBuffer = k.write(keyType, key);
            ByteBuffer value = (ByteBuffer) c.get(getName(), keyBuffer).get();
            if (value == null)
                return null;
            return valueType.read(value);
        }
    }

    @Override
    public Object replicationAppend(Session session, Object value, StorageDataType valueType) {
        List<NetNode> replicationNodes = getLastPageReplicationNodes();
        ReplicationSession rs = db.createReplicationSession(session, replicationNodes);
        try (DataBuffer v = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer valueBuffer = v.write(valueType, value);
            Future<Object> f = c.append(getName(), valueBuffer);
            return f.get();
        }
    }

    @Override
    public synchronized LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        BTreePage p = root.binarySearchLeafPage(leafPageMovePlan.pageKey.key);
        if (p.isLeaf()) {
            // 老的index < 新的index时，说明上一次没有达成一致，进行第二次协商
            if (p.getLeafPageMovePlan() == null || p.getLeafPageMovePlan().getIndex() < leafPageMovePlan.getIndex()) {
                p.setLeafPageMovePlan(leafPageMovePlan);
            }
            return p.getLeafPageMovePlan();
        }
        return null;
    }

    public void replicateRootPage(DataBuffer p) {
        root.replicatePage(p, NetNode.getLocalTcpNode());
    }

    public void setOldNodes(String[] oldNodes) {
        this.oldNodes = oldNodes;
    }

    public void setDatabase(IDatabase db) {
        this.db = db;
    }

    public void setRunMode(RunMode runMode) {
        isShardingMode = runMode == RunMode.SHARDING;
    }

    boolean isShardingMode() {
        return isShardingMode;
    }

    @Override
    public ByteBuffer readPage(PageKey pageKey) {
        BTreePage p = root;
        Object k = pageKey.key;
        if (p.isLeaf()) {
            throw DbException.throwInternalError("readPage: pageKey=" + pageKey);
        }
        BTreePage parent = p;
        int index = 0;
        while (p.isNode()) {
            index = p.binarySearch(k);
            if (index < 0) {
                index = -index - 1;
                parent = p;
                p = p.getChildPage(index);
            } else {
                // 第一和第二个child使用同一个pageKey，
                // 如果此时first为true，就不需要增加index了
                if (!pageKey.first)
                    index++;
                return replicateOrMovePage(pageKey, parent.getChildPage(index), parent, index);
            }
        }
        return null;
    }

    private ByteBuffer replicatePage(BTreePage p) {
        try (DataBuffer buff = DataBuffer.create()) {
            p.replicatePage(buff, getLocalNode());
            ByteBuffer pageBuffer = buff.getAndCopyBuffer();
            return pageBuffer;
        }
    }

    private ByteBuffer replicateOrMovePage(PageKey pageKey, BTreePage p, BTreePage parent, int index) {
        // 从client_server模式到replication模式
        if (!isShardingMode()) {
            return replicatePage(p);
        }

        // 如果该page已经处理过，那么直接返回它
        if (p.getReplicationHostIds() != null) {
            return replicatePage(p);
        }

        // 以下处理从client_server或replication模式到sharding模式的场景
        // ---------------------------------------------------------------
        replicateOrMovePage(pageKey, p, parent, index, oldNodes, true);

        return replicatePage(p);
    }

    private static List<String> toHostIds(IDatabase db, List<NetNode> nodes) {
        List<String> hostIds = new ArrayList<>(nodes.size());
        for (NetNode e : nodes) {
            String id = db.getHostId(e);
            hostIds.add(id);
        }
        return hostIds;
    }

    public synchronized void readRootPageFrom(ByteBuffer data) {
        root = BTreePage.readReplicatedPage(this, data);
        if (root.isNode() && !getName().endsWith("_0")) { // 只异步读非SYS表
            root.readRemotePages();
        }
    }

    public void replicateAllRemotePages() {
        root.readRemotePagesRecursive();
    }

    public void moveAllLocalLeafPages(String[] oldNodes, String[] newNodes) {
        root.moveAllLocalLeafPages(oldNodes, newNodes);
    }

    // 查找闭区间[from, to]对应的所有leaf page，并建立这些leaf page所在节点与page key的映射关系
    // 该方法不需要读取leaf page或remote page
    @Override
    public Map<String, List<PageKey>> getNodeToPageKeyMap(Session session, K from, K to) {
        return getNodeToPageKeyMap(session, from, to, null);
    }

    public Map<String, List<PageKey>> getNodeToPageKeyMap(Session session, K from, K to, List<PageKey> pageKeys) {
        Map<String, List<PageKey>> map = new HashMap<>();
        Random random = new Random();
        if (root.isLeaf()) {
            Object key = root.getKeyCount() == 0 ? null : root.getKey(0);
            getPageKey(map, random, pageKeys, root, 0, key);
        } else {
            dfs(map, random, from, to, pageKeys);
        }
        return map;
    }

    // 深度优先搜索(不使用递归)
    private void dfs(Map<String, List<PageKey>> map, Random random, K from, K to, List<PageKey> pageKeys) {
        CursorPos pos = null;
        BTreePage p = root;
        while (p.isNode()) {
            // 注意: index是子page的数组索引，不是keys数组的索引
            int index = from == null ? -1 : p.binarySearch(from);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            pos = new CursorPos(p, index + 1, pos);
            if (p.isNodeChildPage(index)) {
                p = p.getChildPage(index);
            } else {
                getPageKeys(map, random, from, to, pageKeys, p, index);

                // from此时为null，代表从右边兄弟节点keys数组的0号索引开始
                from = null;
                // 转到上一层，遍历右边的兄弟节点
                for (;;) {
                    pos = pos.parent;
                    if (pos == null) {
                        return;
                    }
                    if (pos.index < getChildPageCount(pos.page)) {
                        if (pos.page.isNodeChildPage(pos.index)) {
                            p = pos.page.getChildPage(pos.index++);
                            break; // 只是退出for循环
                        }
                    }
                }
            }
        }
    }

    private void getPageKeys(Map<String, List<PageKey>> map, Random random, K from, K to, List<PageKey> pageKeys,
            BTreePage p, int index) {
        int keyCount = p.getKeyCount();
        if (keyCount > 1) {
            boolean needsCompare = false;
            if (to != null) {
                Object lastKey = p.getLastKey();
                if (keyType.compare(lastKey, to) >= 0) {
                    needsCompare = true;
                }
            }
            // node page的直接子page不会出现同时包含node page和leaf page的情况
            for (int size = getChildPageCount(p); index < size; index++) {
                int keyIndex = index - 1;
                Object k = p.getKey(keyIndex < 0 ? 0 : keyIndex);
                if (needsCompare && keyType.compare(k, to) > 0) {
                    return;
                }
                getPageKey(map, random, pageKeys, p, index, k);
            }
        } else if (keyCount == 1) {
            Object k = p.getKey(0);
            if (from == null || keyType.compare(from, k) < 0) {
                getPageKey(map, random, pageKeys, p, 0, k);
            }
            if ((from != null && keyType.compare(from, k) >= 0) //
                    || to == null //
                    || keyType.compare(to, k) >= 0) {
                getPageKey(map, random, pageKeys, p, 1, k);
            }
        } else { // 当keyCount=0时也是合法的，比如node page只删到剩一个leaf page时
            if (getChildPageCount(p) != 1) {
                throw DbException.throwInternalError();
            }
            Object k = p.getChildPageReference(0).pageKey.key;
            getPageKey(map, random, pageKeys, p, 0, k);
        }
    }

    private void getPageKey(Map<String, List<PageKey>> map, Random random, List<PageKey> pageKeys, BTreePage p,
            int index, Object key) {
        long pos;
        List<String> hostIds;
        if (p.isNode()) {
            PageReference pr = p.getChildPageReference(index);
            pos = pr.pos;
            hostIds = pr.replicationHostIds;
        } else {
            pos = p.getPos();
            hostIds = p.getReplicationHostIds();
        }

        PageKey pk = new PageKey(key, index == 0, pos);
        if (pageKeys != null)
            pageKeys.add(pk);
        if (hostIds != null) {
            int i = random.nextInt(hostIds.size());
            String hostId = hostIds.get(i);
            List<PageKey> keys = map.get(hostId);
            if (keys == null) {
                keys = new ArrayList<>();
                map.put(hostId, keys);
            }
            keys.add(pk);
        }
    }
}
