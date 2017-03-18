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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.Gossiper;
import org.lealone.aose.server.P2pServer;
import org.lealone.aose.storage.AOStorage;
import org.lealone.aose.storage.AOStorageEngine;
import org.lealone.aose.storage.StorageMapBuilder;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.Session;
import org.lealone.db.SessionPool;
import org.lealone.net.NetEndpoint;
import org.lealone.replication.Replication;
import org.lealone.replication.ReplicationSession;
import org.lealone.storage.Storage;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.StorageMapBase;
import org.lealone.storage.StorageMapCursor;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.WriteBuffer;
import org.lealone.storage.type.WriteBufferPool;

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
public class BTreeMap<K, V> extends StorageMapBase<K, V> implements Replication {

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
    protected final Database db;

    /**
     * The current root page (may not be null).
     */
    protected volatile BTreePage root;

    @SuppressWarnings("unchecked")
    protected BTreeMap(String name, DataType keyType, DataType valueType, Map<String, Object> config,
            AOStorage aoStorage) {
        super(name, keyType, valueType);
        DataUtils.checkArgument(config != null, "The config may not be null");

        this.readOnly = config.containsKey("readOnly");
        this.isShardingMode = config.containsKey("isShardingMode");
        this.config = config;
        this.aoStorage = aoStorage;
        this.db = (Database) config.get("db");

        storage = new BTreeStorage((BTreeMap<Object, Object>) this);

        if (storage.lastChunk != null)
            root = storage.readPage(storage.lastChunk.rootPagePos);
        else {
            root = BTreePage.createEmpty(this);
            if (isShardingMode) {
                // 后面加入的新节点对应的host_id必须大于现有的，否则这里可能会导致错误
                // 例如节点A执行完DDL后，加入了新节点B，然后在老的节点C上执行DDL，此时节点C就可能看到节点B的host_id了
                ArrayList<Integer> hostIds = P2pServer.instance.getTopologyMetaData().sortedHostIds();
                if (!hostIds.isEmpty()) {
                    Integer hostId = hostIds.get(Math.abs(name.hashCode() % hostIds.size()));
                    List<NetEndpoint> replicationEndpoints = P2pServer.instance.getReplicationEndpoints(db, hostId);
                    int size = replicationEndpoints.size();
                    root.replicationHostIds = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        root.replicationHostIds
                                .add(P2pServer.instance.getTopologyMetaData().getHostId(replicationEndpoints.get(i)));
                    }
                }
            }
        }
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
        if (split && isShardingMode && root.isLeaf())
            moveLeafPage(p.getKey(0), p.getChildPage(1));

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
    protected BTreePage splitRoot(BTreePage p) {
        long totalCount = p.getTotalCount();
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        BTreePage split = p.split(at);
        if (p.isLeaf())
            split.replicationHostIds = p.replicationHostIds;
        Object[] keys = { k };
        BTreePage.PageReference[] children = { new BTreePage.PageReference(p, p.getPos(), p.getTotalCount()),
                new BTreePage.PageReference(split, split.getPos(), split.getTotalCount()), };
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
    protected Object put(BTreePage p, Object key, Object value) {
        int index = p.binarySearch(key);
        if (p.isLeaf()) {
            if (index < 0) {
                index = -index - 1;
                p.insertLeaf(index, key, value);
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
            BTreePage split = c.split(at);
            p.setChild(index, split);
            p.insertNode(index, k, c);
            // now we are not sure where to add
            // return put(p, key, value);
            Object result = put(p, key, value);
            if (isLeaf)
                moveLeafPage(k, split);
            return result;
        }
        Object result = put(c, key, value);
        p.setChild(index, c);
        return result;
    }

    private void moveLeafPage(Object splitKey, BTreePage rightChildPage) {
        if (isShardingMode && rightChildPage.replicationHostIds.get(0).equals(P2pServer.instance.getLocalHostId())) {
            Integer hostId = rightChildPage.replicationHostIds.get(0);
            Integer nextHostId = P2pServer.instance.getTopologyMetaData().getNextHostId(hostId);
            List<NetEndpoint> newReplicationEndpoints = P2pServer.instance.getReplicationEndpoints(db, nextHostId);
            List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(rightChildPage);
            newReplicationEndpoints.remove(getLocalEndpoint());
            oldReplicationEndpoints.remove(getLocalEndpoint());
            Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
            liveMembers.removeAll(newReplicationEndpoints);
            liveMembers.removeAll(oldReplicationEndpoints);

            int size = newReplicationEndpoints.size();
            List<Integer> moveTo = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                moveTo.add(P2pServer.instance.getTopologyMetaData().getHostId(newReplicationEndpoints.get(i)));
            }
            rightChildPage.replicationHostIds = moveTo;

            // move page
            Session session = ConnectionInfo.getAndRemoveInternalSession();
            Session[] sessions = new Session[size];
            ServerSession s = (ServerSession) session;
            int i = 0;
            for (NetEndpoint ia : newReplicationEndpoints)
                sessions[i++] = SessionPool.getSession(s, s.getURL(ia.geInetAddress()));

            ReplicationSession rs = new ReplicationSession(sessions);
            rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
            moveLeafPage(splitKey, rightChildPage, rs, false);

            // split root page
            i = 0;
            size = liveMembers.size();
            sessions = new Session[size];
            for (NetEndpoint ia : liveMembers)
                sessions[i++] = SessionPool.getSession(s, s.getURL(ia.geInetAddress()));

            rs = new ReplicationSession(sessions);
            rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
            moveLeafPage(splitKey, rightChildPage, rs, true);
        }
    }

    private void moveLeafPage(Object splitKey, BTreePage rightChildPage, ReplicationSession rs, boolean remote) {
        StorageCommand c = null;
        try {
            c = rs.createStorageCommand();

            WriteBuffer writeBuffer = WriteBufferPool.poll();
            keyType.write(writeBuffer, splitKey);
            ByteBuffer buffer = writeBuffer.getBuffer();
            buffer.flip();
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(buffer.limit());
            keyBuffer.put(buffer);
            keyBuffer.flip();

            writeBuffer.clear();

            rightChildPage.writeLeaf(writeBuffer, remote);
            buffer = writeBuffer.getBuffer();
            buffer.flip();
            ByteBuffer pageBuffer = ByteBuffer.allocateDirect(buffer.limit());
            pageBuffer.put(buffer);
            pageBuffer.flip();
            WriteBufferPool.offer(writeBuffer);
            c.moveLeafPage(getName(), keyBuffer, pageBuffer);
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
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
            List<NetEndpoint> oldReplicationEndpoints = getReplicationEndpoints(leafPage);
            oldReplicationEndpoints.remove(getLocalEndpoint());
            Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
            liveMembers.removeAll(oldReplicationEndpoints);

            int i = 0;
            int size = liveMembers.size();
            Session session = ConnectionInfo.getAndRemoveInternalSession();
            Session[] sessions = new Session[size];
            ServerSession s = (ServerSession) session;
            for (NetEndpoint ia : liveMembers)
                sessions[i++] = SessionPool.getSession(s, s.getURL(ia.geInetAddress()));

            ReplicationSession rs = new ReplicationSession(sessions);
            rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
            StorageCommand c = null;
            try {
                c = rs.createStorageCommand();

                WriteBuffer writeBuffer = WriteBufferPool.poll();
                keyType.write(writeBuffer, key);
                ByteBuffer buffer = writeBuffer.getBuffer();
                buffer.flip();
                ByteBuffer keyBuffer = ByteBuffer.allocateDirect(buffer.limit());
                keyBuffer.put(buffer);
                keyBuffer.flip();
                c.removeLeafPage(getName(), keyBuffer);
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                if (c != null)
                    c.close();
            }
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
        int size = p.replicationHostIds.size();
        List<NetEndpoint> replicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            replicationEndpoints
                    .add(P2pServer.instance.getTopologyMetaData().getEndpointForHostId(p.replicationHostIds.get(i)));
        }
        return replicationEndpoints;
    }

    @Override
    public NetEndpoint getLocalEndpoint() {
        return ConfigDescriptor.getLocalEndpoint();
    }

    @Override
    public Object put(Object key, Object value, DataType valueType, Session session) {
        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(key);
        NetEndpoint localEndpoint = getLocalEndpoint();

        Session[] sessions = new Session[replicationEndpoints.size()];
        ServerSession s = (ServerSession) session;
        int i = 0;
        for (NetEndpoint ia : replicationEndpoints) {
            sessions[i++] = SessionPool.getSession(s, s.getURL(ia.geInetAddress()), !localEndpoint.equals(ia));
        }

        ReplicationSession rs = new ReplicationSession(sessions);
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
        StorageCommand c = null;
        try {
            c = rs.createStorageCommand();
            WriteBuffer writeBuffer = WriteBufferPool.poll();
            keyType.write(writeBuffer, key);
            ByteBuffer buffer = writeBuffer.getBuffer();
            buffer.flip();
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(buffer.limit());
            keyBuffer.put(buffer);
            keyBuffer.flip();

            writeBuffer.clear();
            valueType.write(writeBuffer, value);
            buffer = writeBuffer.getBuffer();
            buffer.flip();
            ByteBuffer valueBuffer = ByteBuffer.allocateDirect(buffer.limit());
            valueBuffer.put(buffer);
            valueBuffer.flip();
            WriteBufferPool.offer(writeBuffer);
            byte[] oldValue = (byte[]) c.executePut(AOStorageEngine.NAME, getName(), keyBuffer, valueBuffer);
            if (oldValue == null)
                return null;
            return valueType.read(ByteBuffer.wrap(oldValue));
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

    @Override
    public Object get(Object key, Session session) {
        List<NetEndpoint> replicationEndpoints = getReplicationEndpoints(key);
        NetEndpoint localEndpoint = getLocalEndpoint();

        Session[] sessions = new Session[replicationEndpoints.size()];
        ServerSession s = (ServerSession) session;
        int i = 0;
        for (NetEndpoint ia : replicationEndpoints)
            sessions[i++] = SessionPool.getSession(s, s.getURL(ia.geInetAddress()), !localEndpoint.equals(ia));

        ReplicationSession rs = new ReplicationSession(sessions);
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
        StorageCommand c = null;
        try {
            c = rs.createStorageCommand();
            WriteBuffer writeBuffer = WriteBufferPool.poll();
            keyType.write(writeBuffer, key);
            ByteBuffer buffer = writeBuffer.getBuffer();
            buffer.flip();
            ByteBuffer keyBuffer = ByteBuffer.allocateDirect(buffer.limit());
            keyBuffer.put(buffer);
            keyBuffer.flip();
            byte[] value = (byte[]) c.executeGet(getName(), keyBuffer);
            if (value == null)
                return null;
            return valueType.read(ByteBuffer.wrap(value));
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

    @Override
    public synchronized void addLeafPage(ByteBuffer splitKey, ByteBuffer page) {
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
            while (true) {
                index = p.binarySearch(splitKey);
                if (!p.isLeaf()) {
                    if (index < 0) {
                        index = -index - 1;
                    } else {
                        index++;
                    }
                    parent = p;
                    p = p.getChildPage(index);
                    continue;
                }
                break;
            }
            BTreePage right = BTreePage.readLeafPage(this, page);
            parent.insertNode(index, k, right);
        }
    }

    @Override
    public synchronized void removeLeafPage(ByteBuffer key) {
        beforeWrite();
        synchronized (this) {
            BTreePage p = root.copy();
            removeLeafPage(p, key);
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
}
