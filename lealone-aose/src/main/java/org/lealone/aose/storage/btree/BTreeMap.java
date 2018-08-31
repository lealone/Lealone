/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.aose.storage.btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.concurrent.LealoneExecutorService;
import org.lealone.aose.concurrent.Stage;
import org.lealone.aose.concurrent.StageManager;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.locator.TopologyMetaData;
import org.lealone.aose.router.P2pRouter;
import org.lealone.aose.server.P2pServer;
import org.lealone.aose.storage.AOStorage;
import org.lealone.aose.storage.AOStorageService;
import org.lealone.aose.storage.StorageMapBuilder;
import org.lealone.aose.storage.btree.BTreePage.PageReference;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.Database;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.value.ValueLong;
import org.lealone.net.NetEndpoint;
import org.lealone.replication.ReplicationSession;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.StorageDataType;

/**
 * A read optimization BTree stored map.
 * <p>
 * Read operations can happen concurrently with all other operations, without
 * risk of corruption.
 * <p>
 * Write operations first read the relevant area from disk to memory
 * concurrently, and only then modify the data. The in-memory part of write
 * operations is synchronized. For scalable concurrent in-memory write
 * operations, the map should be split into multiple smaller sub-maps that are
 * then synchronized independently.
 * 
 * @param <K> the key class
 * @param <V> the value class
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeMap<K, V> extends StorageMapBase<K, V> {

    /**
     * A builder for this class.
     */
    public static class Builder<K, V> extends StorageMapBuilder<BTreeMap<K, V>, K, V> {
        @Override
        public BTreeMap<K, V> openMap() {
            return new BTreeMap<>(name, keyType, valueType, config, aoStorage);
        }
    }

    protected final boolean readOnly;
    protected final boolean isShardingMode;

    protected final Map<String, Object> config;
    protected final BTreeStorage storage;
    protected final AOStorage aoStorage;
    protected Database db;

    /**
     * The current root page (may not be null).
     */
    protected volatile BTreePage root;

    private RunMode runMode;
    private String[] oldEndpoints;

    @SuppressWarnings("unchecked")
    protected BTreeMap(String name, StorageDataType keyType, StorageDataType valueType, Map<String, Object> config,
            AOStorage aoStorage) {
        super(name, keyType, valueType);
        DataUtils.checkArgument(config != null, "The config may not be null");

        this.readOnly = config.containsKey("readOnly");
        boolean isShardingMode = config.containsKey("isShardingMode");
        this.config = config;
        this.aoStorage = aoStorage;
        this.db = (Database) config.get("db");

        storage = new BTreeStorage((BTreeMap<Object, Object>) this);

        if (storage.lastChunk != null) {
            root = storage.readPage(storage.lastChunk.rootPagePos);
            setLastKey(lastKey());
        } else {
            root = BTreePage.createEmpty(this);
            String initReplicationEndpoints = (String) config.get("initReplicationEndpoints");
            // DataUtils.checkArgument(initReplicationEndpoints != null, "The initReplicationEndpoints may not be
            // null");
            if (isShardingMode && initReplicationEndpoints != null) {
                String[] replicationEndpoints = StringUtils.arraySplit(initReplicationEndpoints, '&');
                root.replicationHostIds = Arrays.asList(replicationEndpoints);
                storage.addHostIds(replicationEndpoints);
                // 强制把replicationHostIds持久化
                storage.forceSave();
            } else {
                isShardingMode = false;
            }
        }

        this.isShardingMode = isShardingMode;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        return (V) binarySearch(root, key);
    }

    /**
     * Get the value for the given key, or null if not found.
     * 
     * @param p the page
     * @param key the key
     * @return the value or null
     */
    protected Object binarySearch(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        if (!p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            p = p.getChildPage(index);
            return binarySearch(p, key);
        }
        if (index >= 0) {
            return p.getValue(index);
        }
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized V put(K key, V value) {
        DataUtils.checkArgument(value != null, "The value may not be null");

        beforeWrite();
        BTreePage p = root.copy();

        boolean split = false;
        if (p.needSplit()) {
            p = splitRoot(p);
            split = true;
        }

        Object result = put(p, key, value);
        if (split && isShardingMode && root.isLeaf()) {
            moveLeafPageLazy(p.getKey(0));
        }

        newRoot(p);
        return (V) result;
    }

    /**
     * This method is called before writing to the map. 
     * The default implementation checks whether writing is allowed.
     * 
     * @throws UnsupportedOperationException if the map is read-only.
     */
    protected void beforeWrite() {
        if (storage.isClosed()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This map is closed");
        }
        if (readOnly) {
            throw DataUtils.newUnsupportedOperationException("This map is read-only");
        }
    }

    /**
     * Split the root page.
     * 
     * @param p the page
     * @return the new root page
     */
    private BTreePage splitRoot(BTreePage p) {
        long totalCount = p.getTotalCount();
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        BTreePage rightChildPage = p.split(at);
        Object[] keys = { k };
        BTreePage.PageReference[] children = { new BTreePage.PageReference(p, p.getPos(), p.getTotalCount()),
                new BTreePage.PageReference(rightChildPage, rightChildPage.getPos(), rightChildPage.getTotalCount()) };
        p = BTreePage.create(this, keys, null, children, totalCount, 0);
        return p;
    }

    /**
     * Add or update a key-value pair.
     * 
     * @param p the page
     * @param key the key (may not be null)
     * @param value the value (may not be null)
     * @return the old value, or null
     */
    private Object put(BTreePage p, Object key, Object value) {
        // 本地后台批量put时(比如通过BufferedMap执行)，可能会发生leafPage切割，
        // 这时复制节点就发生改变了，需要重定向到新的复制节点
        // 比如下面这样的场景就会发生:
        // AOStorageService执行完merge后，正准备执行page move，此时又有新数据写入BufferedMap，
        // 当下次merge执行到put时，page所在节点就可能不是当前节点了
        if (p.leafPageMovePlan != null) {
            return putRemote(p, key, value);
        } else {
            return putLocal(p, key, value);
        }
    }

    private Object putRemote(BTreePage p, Object key, Object value) {
        if (p.leafPageMovePlan.moverHostId.equals(P2pServer.instance.getLocalHostId())) {
            int size = p.leafPageMovePlan.replicationEndpoints.size();
            List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
            replicationEndpoints.addAll(p.leafPageMovePlan.replicationEndpoints);
            boolean containsLocalEndpoint = replicationEndpoints.remove(getLocalEndpoint());
            Object returnValue = null;
            ReplicationSession rs = P2pRouter.createReplicationSession(db.createInternalSession(),
                    replicationEndpoints);
            try (DataBuffer k = DataBuffer.create();
                    DataBuffer v = DataBuffer.create();
                    StorageCommand c = rs.createStorageCommand()) {
                ByteBuffer keyBuffer = k.write(keyType, key);
                ByteBuffer valueBuffer = v.write(valueType, value);
                byte[] oldValue = (byte[]) c.executePut(null, getName(), keyBuffer, valueBuffer, true);
                if (oldValue != null) {
                    returnValue = valueType.read(ByteBuffer.wrap(oldValue));
                }
            }
            // 如果新的复制节点中还包含本地节点，那么还需要put到本地节点中
            if (containsLocalEndpoint) {
                return putLocal(p, key, value);
            } else {
                return returnValue;
            }
        } else {
            return null; // 不是由当前节点移动的，那么put操作就可以忽略了
        }
    }

    private Object putLocal(BTreePage p, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
                setLastKey(key);
                return null;
            }
            return p.setValue(index, value);
        }
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        BTreePage c = p.getChildPage(index).copy();
        if (c.needSplit()) {
            boolean isLeaf = c.isLeaf();
            // split on the way down
            int at = c.getKeyCount() / 2;
            Object k = c.getKey(at);
            BTreePage rightChildPage = c.split(at);
            p.setChild(index, rightChildPage);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            Object result = put(p, key, value);
            if (isLeaf && isShardingMode) {
                moveLeafPageLazy(k);
            }
            return result;
        }
        Object result = put(c, key, value);
        p.setChild(index, c);
        return result;
    }

    private Set<NetEndpoint> getCandidateEndpoints() {
        String[] hostIds = db.getHostIds();
        Set<NetEndpoint> candidateEndpoints = new HashSet<>(hostIds.length);
        TopologyMetaData metaData = P2pServer.instance.getTopologyMetaData();
        for (String hostId : hostIds) {
            candidateEndpoints.add(metaData.getEndpointForHostId(hostId));
        }
        return candidateEndpoints;
    }

    // 必需同步
    private synchronized BTreePage setLeafPageMovePlan(Object splitKey, LeafPageMovePlan leafPageMovePlan) {
        BTreePage page = root.binarySearchLeafPage(root, splitKey);
        if (page != null) {
            page.leafPageMovePlan = leafPageMovePlan;
        }
        return page;
    }

    private void moveLeafPageLazy(Object keyObejct) {
        AOStorageService.addPendingTask(() -> {
            moveLeafPage(keyObejct);
            return null;
        });
    }

    private void moveLeafPage(Object keyObejct) {
        ByteBuffer keyBuffer;
        try (DataBuffer buff = DataBuffer.create()) {
            getKeyType().write(buff, keyObejct);
            keyBuffer = buff.getAndCopyBuffer();
        }
        BTreePage p = root;
        BTreePage parent = p;
        int index = 0;
        while (p.isNode()) {
            index = p.binarySearch(keyObejct);
            if (index < 0) {
                index = -index - 1;
                parent = p;
                p = p.getChildPage(index);
            } else {
                index++;
                p = parent.getChildPage(index);
                BTreePage left = parent.getChildPage(index - 1);
                // 左边已经移动过了，那么右边就不需要再移
                if (p.replicationHostIds != left.replicationHostIds) {
                    return;
                }
                String[] oldEndpoints;
                if (p.replicationHostIds == null) {
                    oldEndpoints = new String[0];
                } else {
                    oldEndpoints = new String[p.replicationHostIds.size()];
                    p.replicationHostIds.toArray(oldEndpoints);
                }
                replicateOrMovePage(keyObejct, keyBuffer, p, parent, index, true, oldEndpoints, false);
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
    // 除此之外，这三者并没有多大差异，只是oldEndpoints中包含的节点个数多少的问题，
    // client_server模式只有一个节点，在replication模式下，如果副本个数是1，那么也相当于client_server模式。
    private void replicateOrMovePage(Object keyObejct, ByteBuffer keyBuffer, BTreePage p, BTreePage parent, int index,
            boolean last, String[] oldEndpoints, boolean replicate) {
        if (oldEndpoints == null || oldEndpoints.length == 0) {
            DbException.throwInternalError("oldEndpoints is null");
        }
        Set<NetEndpoint> candidateEndpoints = getCandidateEndpoints();
        List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(oldEndpoints);
        Set<NetEndpoint> oldEndpointSet;
        if (replicate) {
            // 允许选择原来的节点，所以用new HashSet<>(0)替代new HashSet<>(oldReplicationEndpoints)
            oldEndpointSet = new HashSet<>(0);
        } else {
            oldEndpointSet = new HashSet<>(oldReplicationEndpoints);
        }

        List<NetEndpoint> newReplicationEndpoints = P2pServer.instance.getReplicationEndpoints(db, oldEndpointSet,
                candidateEndpoints);

        Session session = db.createInternalSession();
        LeafPageMovePlan leafPageMovePlan = null;

        if (oldEndpoints.length == 1) {
            leafPageMovePlan = new LeafPageMovePlan(oldEndpoints[0], newReplicationEndpoints, keyBuffer);
            p.leafPageMovePlan = leafPageMovePlan;
        } else {
            ReplicationSession rs = P2pRouter.createReplicationSession(session, oldReplicationEndpoints);
            try (DataBuffer k = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
                ByteBuffer key = k.write(keyType, keyObejct);
                LeafPageMovePlan plan = new LeafPageMovePlan(P2pServer.instance.getLocalHostId(),
                        newReplicationEndpoints, key);
                leafPageMovePlan = c.prepareMoveLeafPage(getName(), plan);
            }

            if (leafPageMovePlan == null)
                return;

            // 重新按key找到page，因为经过前面的操作后，
            // 可能page已经有新数据了，如果只移动老的，会丢失数据
            p = setLeafPageMovePlan(keyObejct, leafPageMovePlan);

            if (!leafPageMovePlan.moverHostId.equals(P2pServer.instance.getLocalHostId())) {
                p.replicationHostIds = leafPageMovePlan.getReplicationEndpoints();
                return;
            }
        }

        p.replicationHostIds = toHostIds(newReplicationEndpoints);
        NetEndpoint localEndpoint = getLocalEndpoint();

        Set<NetEndpoint> otherEndpoints = new HashSet<>(candidateEndpoints);
        otherEndpoints.removeAll(newReplicationEndpoints);
        if (!replicate) {
            otherEndpoints.removeAll(oldReplicationEndpoints);
            newReplicationEndpoints.removeAll(oldReplicationEndpoints);
        }

        if (newReplicationEndpoints.contains(localEndpoint)) {
            newReplicationEndpoints.remove(localEndpoint);
        }

        // 移动page到新的复制节点(page中包含数据)
        if (!newReplicationEndpoints.isEmpty()) {
            ReplicationSession rs = P2pRouter.createReplicationSession(session, newReplicationEndpoints, true);
            moveLeafPage(leafPageMovePlan, p, rs, false, last, !replicate);
        }

        // 当前节点已经不是副本所在节点
        if (replicate && otherEndpoints.contains(localEndpoint)) {
            otherEndpoints.remove(localEndpoint);
            PageReference r = new PageReference(null, -1, 0);
            r.key = parent.getKey(index);
            r.last = index == parent.getKeyCount() - 1;
            r.remoteHostIds = p.replicationHostIds;
            parent.setChild(index, r);
        }

        // 移动page到其他节点(page中不包含数据，只包含这个page各数据副本所在节点信息)
        if (!otherEndpoints.isEmpty()) {
            ReplicationSession rs = P2pRouter.createReplicationSession(session, otherEndpoints, true);
            moveLeafPage(leafPageMovePlan, p, rs, true, last, !replicate);
        }
    }

    private void moveLeafPage(LeafPageMovePlan leafPageMovePlan, BTreePage page, ReplicationSession rs, boolean remote,
            boolean last, boolean addPage) {
        try (DataBuffer buff = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            page.writeLeaf(buff, remote);
            ByteBuffer pageBuffer = buff.getAndFlipBuffer();
            c.moveLeafPage(getName(), leafPageMovePlan.splitKey, pageBuffer, last, addPage);
        }
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
    public synchronized V putIfAbsent(K key, V value) {
        V old = get(key);
        if (old == null) {
            put(key, value);
        }
        return old;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V remove(K key) {
        beforeWrite();
        V result = get(key);
        if (result == null) {
            return null;
        }
        synchronized (this) {
            BTreePage p = root.copy();
            result = (V) remove(p, key);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p.removePage();
                p = BTreePage.createEmpty(this);
            }
            newRoot(p);
        }
        return result;
    }

    /**
     * Remove a key-value pair.
     * 
     * @param p the page (may not be null)
     * @param key the key
     * @return the old value, or null if the key did not exist
     */
    protected Object remove(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {
            if (index >= 0) {
                result = p.getValue(index);
                p.remove(index);
            }
            return result;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        BTreePage cOld = p.getChildPage(index);
        BTreePage c = cOld.copy();
        result = remove(c, key);
        if (result == null || c.getTotalCount() != 0) {
            // no change, or
            // there are more nodes
            p.setChild(index, c);
        } else {
            // this child was deleted
            if (p.getKeyCount() == 0) { // 如果p的子节点只剩一个叶子节点时，keyCount为0
                p.setChild(index, c);
                c.removePage();
            } else {
                p.remove(index);
            }
            if (c.isLeaf() && isShardingMode)
                removeLeafPage(key, c);
        }
        return result;
    }

    private void removeLeafPage(Object key, BTreePage leafPage) {
        if (leafPage.replicationHostIds.get(0).equals(P2pServer.instance.getLocalHostId())) {
            LealoneExecutorService stage = StageManager.getStage(Stage.REQUEST_RESPONSE);
            stage.execute(() -> {
                List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(leafPage);
                oldReplicationEndpoints.remove(getLocalEndpoint());
                Set<NetEndpoint> liveMembers = getCandidateEndpoints();
                liveMembers.removeAll(oldReplicationEndpoints);
                Session session = db.createInternalSession();
                ReplicationSession rs = P2pRouter.createReplicationSession(session, liveMembers, true);
                try (DataBuffer k = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
                    ByteBuffer keyBuffer = k.write(keyType, key);
                    c.removeLeafPage(getName(), keyBuffer);
                }
            });
        }
    }

    @Override
    public synchronized boolean replace(K key, V oldValue, V newValue) {
        V old = get(key);
        if (areValuesEqual(old, oldValue)) {
            put(key, newValue);
            return true;
        }
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
        if (sizeAsLong() == 0) {
            return null;
        }
        BTreePage p = root;
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
    protected K getMinMax(K key, boolean min, boolean excluding) {
        return getMinMax(root, key, min, excluding);
    }

    @SuppressWarnings("unchecked")
    private K getMinMax(BTreePage p, K key, boolean min, boolean excluding) {
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
    public int size() {
        long size = sizeAsLong();
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }

    @Override
    public long sizeAsLong() {
        return root.getTotalCount();
    }

    @Override
    public boolean containsKey(K key) {
        return get(key) != null;
    }

    @Override
    public boolean isEmpty() {
        return sizeAsLong() == 0;
    }

    @Override
    public boolean isInMemory() {
        return false;
    }

    @Override
    public StorageMapCursor<K, V> cursor(K from) {
        return new BTreeCursor<>(this, root, from);
    }

    @Override
    public synchronized void clear() {
        beforeWrite();
        root.removeAllRecursive();
        newRoot(BTreePage.createEmpty(this));
    }

    @Override
    public void remove() {
        storage.remove();
        aoStorage.closeMap(name);
    }

    @Override
    public boolean isClosed() {
        return storage.isClosed();
    }

    @Override
    public void close() {
        storage.close();
        aoStorage.closeMap(name);
    }

    @Override
    public void save() {
        storage.save();
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public BTreeStorage getBTreeStorage() {
        return storage;
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
    public void transferTo(WritableByteChannel target, K firstKey, K lastKey) throws IOException {
        if (firstKey == null)
            firstKey = firstKey();
        else
            firstKey = ceilingKey(firstKey);

        if (firstKey == null)
            return;

        if (lastKey == null)
            lastKey = lastKey();
        else
            lastKey = floorKey(lastKey);

        if (keyType.compare(firstKey, lastKey) > 0)
            return;

        BTreePage p = root;
        if (p.getTotalCount() > 0) {
            p.transferTo(target, firstKey, lastKey);
        }
    }

    @Override
    public void transferFrom(ReadableByteChannel src) throws IOException {
    }

    // 1.root为空时怎么处理；2.不为空时怎么处理
    public void transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        BTreePage p = root;
        p.transferFrom(src, position, count);
    }

    @Override
    public synchronized void addLeafPage(ByteBuffer splitKey, ByteBuffer page, boolean last, boolean addPage) {
        BTreePage p = root;
        Object k = keyType.read(splitKey);
        if (p.isLeaf()) {
            Object[] keys = { k };
            BTreePage left = BTreePage.createEmpty(this);
            left.replicationHostIds = p.replicationHostIds;

            BTreePage right = BTreePage.readLeafPage(this, page);

            BTreePage.PageReference[] children = {
                    new BTreePage.PageReference(left, left.getPos(), left.getTotalCount()),
                    new BTreePage.PageReference(right, right.getPos(), right.getTotalCount()), };
            p = BTreePage.create(this, keys, null, children, right.getTotalCount(), 0);
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
                    if (last)
                        index++;
                }
                PageReference r = p.getChildPageReference(index);
                if (r.isRemotePage()) {
                    break;
                }
                p = p.getChildPage(index);
            }
            BTreePage right = BTreePage.readLeafPage(this, page);
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
    public synchronized void removeLeafPage(ByteBuffer key) {
        beforeWrite();
        Object k = keyType.read(key);
        synchronized (this) {
            BTreePage p = root.copy();
            removeLeafPage(p, k);
            if (!p.isLeaf() && p.getTotalCount() == 0) {
                p.removePage();
                p = BTreePage.createEmpty(this);
            }
            newRoot(p);
        }
    }

    private void removeLeafPage(BTreePage p, Object key) {
        int index = p.binarySearch(key);
        Object result = null;
        if (p.isLeaf()) {
            return;
        }
        // node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        BTreePage cOld = p.getChildPage(index);
        BTreePage c = cOld.copy();
        result = remove(c, key);
        if (result == null || c.getTotalCount() != 0) {
            // no change, or
            // there are more nodes
            p.setChild(index, c);
        } else {
            // this child was deleted
            if (p.getKeyCount() == 0) { // 如果p的子节点只剩一个叶子节点时，keyCount为0
                p.setChild(index, c);
                c.removePage();
            } else {
                p.remove(index);
            }
        }
    }

    @Override
    public Storage getStorage() {
        return aoStorage;
    }

    @SuppressWarnings("unchecked")
    @Override
    public K append(V value) {
        K key = (K) ValueLong.get(lastKey.incrementAndGet());
        put(key, value);
        return key;
    }

    @Override
    public List<NetEndpoint> getReplicationEndpoints(Object key) {
        return getReplicationEndpoints(root, key);
    }

    private List<NetEndpoint> getReplicationEndpoints(BTreePage p, Object key) {
        if (p.isLeaf()) {
            return getReplicationEndpoints(p);
        }
        int index = p.binarySearch(key);
        // p is a node
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        return getReplicationEndpoints(p.getChildPage(index), key);
    }

    private List<NetEndpoint> getReplicationEndpoints(BTreePage p) {
        return getReplicationEndpoints(p.replicationHostIds);
    }

    List<NetEndpoint> getReplicationEndpoints(String[] replicationHostIds) {
        return getReplicationEndpoints(Arrays.asList(replicationHostIds));
    }

    private List<NetEndpoint> getReplicationEndpoints(List<String> replicationHostIds) {
        int size = replicationHostIds.size();
        List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationEndpoints
                    .add(P2pServer.instance.getTopologyMetaData().getEndpointForHostId(replicationHostIds.get(i)));
        }
        return replicationEndpoints;
    }

    private List<NetEndpoint> getLastPageReplicationEndpoints() {
        BTreePage p = root;
        while (true) {
            if (p.isLeaf()) {
                return getReplicationEndpoints(p);
            }
            p = p.getChildPage(getChildPageCount(p) - 1);
        }
    }

    NetEndpoint getLocalEndpoint() {
        return ConfigDescriptor.getLocalEndpoint();
    }

    @Override
    public Object replicationPut(Session session, Object key, Object value, StorageDataType valueType) {
        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(key);
        ReplicationSession rs = P2pRouter.createReplicationSession(session, replicationEndpoints);
        try (DataBuffer k = DataBuffer.create();
                DataBuffer v = DataBuffer.create();
                StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer keyBuffer = k.write(keyType, key);
            ByteBuffer valueBuffer = v.write(valueType, value);
            byte[] oldValue = (byte[]) c.executePut(null, getName(), keyBuffer, valueBuffer, false);
            if (oldValue == null)
                return null;
            return valueType.read(ByteBuffer.wrap(oldValue));
        }
    }

    @Override
    public Object replicationGet(Session session, Object key) {
        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(key);
        ReplicationSession rs = P2pRouter.createReplicationSession(session, replicationEndpoints);
        try (DataBuffer k = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer keyBuffer = k.write(keyType, key);
            byte[] value = (byte[]) c.executeGet(getName(), keyBuffer);
            if (value == null)
                return null;
            return valueType.read(ByteBuffer.wrap(value));
        }
    }

    @Override
    public Object replicationAppend(Session session, Object value, StorageDataType valueType) {
        List<NetEndpoint> replicationEndpoints = getLastPageReplicationEndpoints();
        ReplicationSession rs = P2pRouter.createReplicationSession(session, replicationEndpoints);
        try (DataBuffer v = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer valueBuffer = v.write(valueType, value);
            return c.executeAppend(null, getName(), valueBuffer, null);
        }
    }

    @Override
    public synchronized LeafPageMovePlan prepareMoveLeafPage(LeafPageMovePlan leafPageMovePlan) {
        Object key = keyType.read(leafPageMovePlan.splitKey.slice());
        BTreePage p = root.binarySearchLeafPage(root, key);
        if (p.isLeaf()) {
            // 老的index < 新的index时，说明上一次没有达成一致，进行第二次协商
            if (p.leafPageMovePlan == null || p.leafPageMovePlan.getIndex() < leafPageMovePlan.getIndex()) {
                p.leafPageMovePlan = leafPageMovePlan;
            }
            return p.leafPageMovePlan;
        }
        return null;
    }

    public void replicateRootPage(DataBuffer p) {
        root.replicatePage(p, NetEndpoint.getLocalTcpEndpoint());
    }

    public void setOldEndpoints(String[] oldEndpoints) {
        this.oldEndpoints = oldEndpoints;
    }

    public void setDatabase(Database db) {
        this.db = db;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    boolean isShardingMode() {
        if (runMode != null) {
            return runMode == RunMode.SHARDING;
        }
        return db.isShardingMode();
    }

    @Override
    public ByteBuffer readPage(ByteBuffer key, boolean last) {
        BTreePage p = root;
        Object k = keyType.read(key);
        key.flip();
        if (p.isLeaf()) {
            throw DbException.throwInternalError("readPage: key=" + key + ", last=" + last);
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
                if (last)
                    index++;
                return replicateOrMovePage(k, key, parent.getChildPage(index), parent, index, last);
            }
        }
        return null;
    }

    private ByteBuffer replicatePage(BTreePage p) {
        try (DataBuffer buff = DataBuffer.create()) {
            p.replicatePage(buff, getLocalEndpoint());
            ByteBuffer pageBuffer = buff.getAndFlipBuffer();
            return pageBuffer.slice();
        }
    }

    private ByteBuffer replicateOrMovePage(Object keyObejct, ByteBuffer keyBuffer, BTreePage p, BTreePage parent,
            int index, boolean last) {
        // 从client_server模式到replication模式
        if (!isShardingMode()) {
            return replicatePage(p);
        }

        // 如果该page已经处理过，那么直接返回它
        if (p.replicationHostIds != null) {
            return replicatePage(p);
        }

        // 以下处理从client_server或replication模式到sharding模式的场景
        // ---------------------------------------------------------------
        replicateOrMovePage(keyObejct, keyBuffer, p, parent, index, last, oldEndpoints, true);

        return replicatePage(p);
    }

    private static List<String> toHostIds(List<NetEndpoint> endpoints) {
        List<String> hostIds = new ArrayList<>(endpoints.size());
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        for (NetEndpoint e : endpoints) {
            String id = md.getHostId(e);
            hostIds.add(id);
        }
        return hostIds;
    }

    @Override
    public synchronized void setRootPage(ByteBuffer buff) {
        root = BTreePage.readReplicatedPage(this, buff);
        if (!root.isLeaf() && !getName().endsWith("_0")) { // 只异步读非SYS表
            root.readRemotePages();
        }
    }
}
