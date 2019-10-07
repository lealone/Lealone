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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
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
import org.lealone.db.Session;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.IterationParameters;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.PageOperations.WriteOperation;
import org.lealone.storage.replication.ReplicationSession;
import org.lealone.storage.type.StorageDataType;

/**
 * 在单机版BTree的基础上支持复制和sharding
 */
public class DistributedBTreeMap<K, V> extends BTreeMap<K, V> implements DistributedStorageMap<K, V> {

    protected final boolean isShardingMode;
    private RunMode runMode;
    private String[] oldEndpoints;

    protected DistributedBTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, Object> config, AOStorage aoStorage) {
        super(name, keyType, valueType, config, aoStorage);

        boolean isShardingMode = false;
        if (config.containsKey("isShardingMode"))
            isShardingMode = Boolean.parseBoolean(config.get("isShardingMode").toString());

        if (btreeStorage.lastChunk == null) {
            if (isShardingMode) {
                String initReplicationEndpoints = (String) config.get("initReplicationEndpoints");
                DataUtils.checkArgument(initReplicationEndpoints != null,
                        "The initReplicationEndpoints may not be null");
                String[] replicationEndpoints = StringUtils.arraySplit(initReplicationEndpoints, '&');
                if (containsLocalEndpoint(replicationEndpoints)) {
                    root = BTreeLeafPage.createEmpty(this);
                } else {
                    root = new BTreeRemotePage(this);
                }
                root.setReplicationHostIds(Arrays.asList(replicationEndpoints));
                btreeStorage.addHostIds(replicationEndpoints);
                // 强制把replicationHostIds持久化
                btreeStorage.forceSave();
                disableParallel = true;
            }
        }
        this.isShardingMode = isShardingMode;
    }

    private boolean containsLocalEndpoint(String[] replicationEndpoints) {
        NetEndpoint local = NetEndpoint.getLocalTcpEndpoint();
        for (String e : replicationEndpoints) {
            if (local.equals(NetEndpoint.createTCP(e)))
                return true;
        }
        return false;
    }

    @Override
    protected void fireRootLeafPageSplit(BTreePage p) {
        PageKey pk = new PageKey(p.getKey(0), false); // 移动右边的Page
        moveLeafPageLazy(pk);
    }

    private String getLocalHostId() {
        return db.getLocalHostId();
    }

    @Override
    protected Object putRemote(BTreePage p, Object key, Object value) {
        if (p.getLeafPageMovePlan().moverHostId.equals(getLocalHostId())) {
            int size = p.getLeafPageMovePlan().replicationEndpoints.size();
            List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
            replicationEndpoints.addAll(p.getLeafPageMovePlan().replicationEndpoints);
            boolean containsLocalEndpoint = replicationEndpoints.remove(getLocalEndpoint());
            Object returnValue = null;
            ReplicationSession rs = db.createReplicationSession(db.createInternalSession(), replicationEndpoints);
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

    @Override
    protected void fireLeafPageSplit(Object k) {
        PageKey pk = new PageKey(k, false); // 移动右边的Page
        moveLeafPageLazy(pk);
    }

    private Set<NetEndpoint> getCandidateEndpoints() {
        return getCandidateEndpoints(db, db.getHostIds());
    }

    static Set<NetEndpoint> getCandidateEndpoints(IDatabase db, String[] hostIds) {
        Set<NetEndpoint> candidateEndpoints = new HashSet<>(hostIds.length);
        for (String hostId : hostIds) {
            candidateEndpoints.add(db.getEndpoint(hostId));
        }
        return candidateEndpoints;
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
        WriteOperation operation = new WriteOperation(() -> {
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
                String[] oldEndpoints;
                if (p.getReplicationHostIds() == null) {
                    oldEndpoints = new String[0];
                } else {
                    oldEndpoints = new String[p.getReplicationHostIds().size()];
                    p.getReplicationHostIds().toArray(oldEndpoints);
                }
                replicateOrMovePage(pageKey, p, parent, index, oldEndpoints, false);
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
    private void replicateOrMovePage(PageKey pageKey, BTreePage p, BTreePage parent, int index, String[] oldEndpoints,
            boolean replicate) {
        Set<NetEndpoint> candidateEndpoints = getCandidateEndpoints();
        replicateOrMovePage(pageKey, p, parent, index, oldEndpoints, replicate, candidateEndpoints);
    }

    void replicateOrMovePage(PageKey pageKey, BTreePage p, BTreePage parent, int index, String[] oldEndpoints,
            boolean replicate, Set<NetEndpoint> candidateEndpoints) {
        if (oldEndpoints == null || oldEndpoints.length == 0) {
            DbException.throwInternalError("oldEndpoints is null");
        }

        List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(db, oldEndpoints);
        Set<NetEndpoint> oldEndpointSet;
        if (replicate) {
            // 允许选择原来的节点，所以用new HashSet<>(0)替代new HashSet<>(oldReplicationEndpoints)
            oldEndpointSet = new HashSet<>(0);
        } else {
            oldEndpointSet = new HashSet<>(oldReplicationEndpoints);
        }

        List<NetEndpoint> newReplicationEndpoints = db.getReplicationEndpoints(oldEndpointSet, candidateEndpoints);

        Session session = db.createInternalSession();
        LeafPageMovePlan leafPageMovePlan = null;

        if (oldEndpoints.length == 1) {
            leafPageMovePlan = new LeafPageMovePlan(oldEndpoints[0], newReplicationEndpoints, pageKey);
            p.setLeafPageMovePlan(leafPageMovePlan);
        } else {
            ReplicationSession rs = db.createReplicationSession(session, oldReplicationEndpoints);
            try (StorageCommand c = rs.createStorageCommand()) {
                LeafPageMovePlan plan = new LeafPageMovePlan(getLocalHostId(), newReplicationEndpoints, pageKey);
                leafPageMovePlan = c.prepareMoveLeafPage(getName(), plan);
            }

            if (leafPageMovePlan == null)
                return;

            // 重新按key找到page，因为经过前面的操作后，
            // 可能page已经有新数据了，如果只移动老的，会丢失数据
            p = setLeafPageMovePlan(pageKey, leafPageMovePlan);

            if (!leafPageMovePlan.moverHostId.equals(getLocalHostId())) {
                p.setReplicationHostIds(leafPageMovePlan.getReplicationEndpoints());
                return;
            }
        }

        p.setReplicationHostIds(toHostIds(db, newReplicationEndpoints));
        NetEndpoint localEndpoint = getLocalEndpoint();

        Set<NetEndpoint> otherEndpoints = new HashSet<>(candidateEndpoints);
        otherEndpoints.removeAll(newReplicationEndpoints);

        if (parent != null && !replicate && !newReplicationEndpoints.contains(localEndpoint)) {
            PageReference r = PageReference.createRemotePageReference(pageKey.key, index == 0);
            r.replicationHostIds = p.getReplicationHostIds();
            parent.setChild(index, r);
        }
        if (!replicate) {
            otherEndpoints.removeAll(oldReplicationEndpoints);
            newReplicationEndpoints.removeAll(oldReplicationEndpoints);
        }

        if (newReplicationEndpoints.contains(localEndpoint)) {
            newReplicationEndpoints.remove(localEndpoint);
        }

        // 移动page到新的复制节点(page中包含数据)
        if (!newReplicationEndpoints.isEmpty()) {
            ReplicationSession rs = db.createReplicationSession(session, newReplicationEndpoints, true);
            moveLeafPage(leafPageMovePlan.pageKey, p, rs, false, !replicate);
        }

        // 当前节点已经不是副本所在节点
        if (parent != null && replicate && otherEndpoints.contains(localEndpoint)) {
            otherEndpoints.remove(localEndpoint);
            PageReference r = PageReference.createRemotePageReference(pageKey.key, index == 0);
            r.replicationHostIds = p.getReplicationHostIds();
            parent.setChild(index, r);
        }

        // 移动page到其他节点(page中不包含数据，只包含这个page各数据副本所在节点信息)
        if (!otherEndpoints.isEmpty()) {
            ReplicationSession rs = db.createReplicationSession(session, otherEndpoints, true);
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

    /**
     * Use the new root page from now on.
     * 
     * @param newRoot the new root page
     */
    @Override
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
            if (p.isNode() && p.isEmpty()) {
                p.removePage();
                p = BTreeLeafPage.createEmpty(this);
            }
            newRoot(p);
        }
        return result;
    }

    @Override
    protected void fireLeafPageRemove(PageKey pageKey, BTreePage leafPage) {
        removeLeafPage(pageKey, leafPage);
    }

    private void removeLeafPage(PageKey pageKey, BTreePage leafPage) {
        if (leafPage.getReplicationHostIds().get(0).equals(getLocalHostId())) {
            WriteOperation operation = new WriteOperation(() -> {
                List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(leafPage);
                Set<NetEndpoint> otherEndpoints = getCandidateEndpoints();
                otherEndpoints.removeAll(oldReplicationEndpoints);
                Session session = db.createInternalSession();
                ReplicationSession rs = db.createReplicationSession(session, otherEndpoints, true);
                try (StorageCommand c = rs.createStorageCommand()) {
                    c.removeLeafPage(getName(), pageKey);
                }
            });
            pohFactory.addPageOperation(operation);
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
    @Override
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
    @Override
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
    protected String getType() {
        return "DistributedBTree";
    }

    // 1.root为空时怎么处理；2.不为空时怎么处理
    public void transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
        ByteBuffer buff = ByteBuffer.allocateDirect((int) count);
        src.read(buff);
        buff.position((int) position);

        while (buff.remaining() > 0) {
            Object key = keyType.read(buff);
            boolean first = buff.get() == 1;
            PageKey pk = new PageKey(key, first);
            addLeafPage(pk, buff, true, true);
            // int pageLength = buff.getInt();
            // pos += pageLength;
            // buff.position(pos);
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
            p = BTreePage.create(this, keys, null, children, new AtomicLong(right.getTotalCount()), 0);
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
        beforeWrite();
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
        return getReplicationEndpoints(db, p.getReplicationHostIds());
    }

    static List<NetEndpoint> getReplicationEndpoints(IDatabase db, String[] replicationHostIds) {
        return getReplicationEndpoints(db, Arrays.asList(replicationHostIds));
    }

    static List<NetEndpoint> getReplicationEndpoints(IDatabase db, List<String> replicationHostIds) {
        int size = replicationHostIds.size();
        List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationEndpoints.add(db.getEndpoint(replicationHostIds.get(i)));
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
        return NetEndpoint.getLocalP2pEndpoint();
    }

    @Override
    public Object replicationPut(Session session, Object key, Object value, StorageDataType valueType) {
        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(key);
        ReplicationSession rs = db.createReplicationSession(session, replicationEndpoints);
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
        ReplicationSession rs = db.createReplicationSession(session, replicationEndpoints);
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
        ReplicationSession rs = db.createReplicationSession(session, replicationEndpoints);
        try (DataBuffer v = DataBuffer.create(); StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer valueBuffer = v.write(valueType, value);
            return c.executeAppend(null, getName(), valueBuffer, null);
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
        root.replicatePage(p, NetEndpoint.getLocalTcpEndpoint());
    }

    public void setOldEndpoints(String[] oldEndpoints) {
        this.oldEndpoints = oldEndpoints;
    }

    public void setDatabase(IDatabase db) {
        this.db = db;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    @Override
    boolean isShardingMode() {
        if (runMode != null) {
            return runMode == RunMode.SHARDING;
        }
        return db.isShardingMode();
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
                index++;
                return replicateOrMovePage(pageKey, parent.getChildPage(index), parent, index);
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
        replicateOrMovePage(pageKey, p, parent, index, oldEndpoints, true);

        return replicatePage(p);
    }

    private static List<String> toHostIds(IDatabase db, List<NetEndpoint> endpoints) {
        List<String> hostIds = new ArrayList<>(endpoints.size());
        for (NetEndpoint e : endpoints) {
            String id = db.getHostId(e);
            hostIds.add(id);
        }
        return hostIds;
    }

    @Override
    public synchronized void setRootPage(ByteBuffer buff) {
        root = BTreePage.readReplicatedPage(this, buff);
        if (root.isNode() && !getName().endsWith("_0")) { // 只异步读非SYS表
            root.readRemotePages();
        }
    }

    public void replicateAllRemotePages() {
        root.readRemotePagesRecursive();
    }

    public void moveAllLocalLeafPages(String[] oldEndpoints, String[] newEndpoints) {
        root.moveAllLocalLeafPages(oldEndpoints, newEndpoints);
    }

    // 查找闭区间[from, to]对应的所有leaf page，并建立这些leaf page所在节点与page key的映射关系
    // 该方法不需要读取leaf page或remote page
    @Override
    public Map<String, List<PageKey>> getEndpointToPageKeyMap(Session session, K from, K to) {
        return getEndpointToPageKeyMap(session, from, to, null);
    }

    public Map<String, List<PageKey>> getEndpointToPageKeyMap(Session session, K from, K to, List<PageKey> pageKeys) {
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

    public static class Builder<K, V> extends BTreeMapBuilder<K, V> {
        @Override
        public DistributedBTreeMap<K, V> openMap() {
            return new DistributedBTreeMap<K, V>(name, keyType, valueType, config, aoStorage);
        }
    }
}
