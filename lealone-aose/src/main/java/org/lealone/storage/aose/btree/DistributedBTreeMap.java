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

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.net.NetNode;
import org.lealone.storage.DistributedStorageMap;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.PageOperations.RunnableOperation;
import org.lealone.storage.replication.ReplicationSession;
import org.lealone.storage.type.StorageDataType;

/**
 * 在单机版BTree的基础上支持复制和sharding
 */
public class DistributedBTreeMap<K, V> extends BTreeMap<K, V> implements DistributedStorageMap<K, V> {

    protected final boolean isShardingMode;
    protected IDatabase db;
    private RunMode runMode;
    private String[] oldNodes;

    protected DistributedBTreeMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, Object> config, AOStorage aoStorage) {
        super(name, keyType, valueType, config, aoStorage);

        if (config.containsKey("isShardingMode"))
            isShardingMode = Boolean.parseBoolean(config.get("isShardingMode").toString());
        else
            isShardingMode = false;
        db = (IDatabase) config.get("db");

        if (btreeStorage.lastChunk == null && isShardingMode) {
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
            parallelDisabled = true;
        }
    }

    private boolean containsLocalNode(String[] replicationNodes) {
        NetNode local = NetNode.getLocalTcpNode();
        for (String e : replicationNodes) {
            if (local.equals(NetNode.createTCP(e)))
                return true;
        }
        return false;
    }

    @Override
    protected IDatabase getDatabase() {
        return db;
    }

    @Override
    protected void fireLeafPageSplit(Object splitKey) {
        PageKey pk = new PageKey(splitKey, false); // 移动右边的Page
        moveLeafPageLazy(pk);
    }

    private String getLocalHostId() {
        return db.getLocalHostId();
    }

    @Override
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
                byte[] oldValue = (byte[]) c.executePut(null, getName(), keyBuffer, valueBuffer, true);
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
                leafPageMovePlan = c.prepareMoveLeafPage(getName(), plan);
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

    @Override
    protected void fireLeafPageRemove(PageKey pageKey, BTreePage leafPage) {
        removeLeafPage(pageKey, leafPage);
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

    static List<NetNode> getReplicationNodes(IDatabase db, String[] replicationHostIds) {
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
            byte[] oldValue = (byte[]) c.executePut(null, getName(), keyBuffer, valueBuffer, false);
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
            byte[] value = (byte[]) c.executeGet(getName(), keyBuffer);
            if (value == null)
                return null;
            return valueType.read(ByteBuffer.wrap(value));
        }
    }

    @Override
    public Object replicationAppend(Session session, Object value, StorageDataType valueType) {
        List<NetNode> replicationNodes = getLastPageReplicationNodes();
        ReplicationSession rs = db.createReplicationSession(session, replicationNodes);
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
        root.replicatePage(p, NetNode.getLocalTcpNode());
    }

    public void setOldNodes(String[] oldNodes) {
        this.oldNodes = oldNodes;
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
            p.replicatePage(buff, getLocalNode());
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

    public static class Builder<K, V> extends BTreeMapBuilder<K, V> {
        @Override
        public DistributedBTreeMap<K, V> openMap() {
            return new DistributedBTreeMap<K, V>(name, keyType, valueType, config, aoStorage);
        }
    }
}
