/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.type.StorageDataType;

/**
 * A leaf page.
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeLeafPage extends BTreeLocalPage {

    /**
     * The values.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] values;

    private List<String> replicationHostIds;
    private LeafPageMovePlan leafPageMovePlan;

    BTreeLeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public List<String> getReplicationHostIds() {
        return replicationHostIds;
    }

    @Override
    public void setReplicationHostIds(List<String> replicationHostIds) {
        this.replicationHostIds = replicationHostIds;
    }

    @Override
    public LeafPageMovePlan getLeafPageMovePlan() {
        return leafPageMovePlan;
    }

    @Override
    public void setLeafPageMovePlan(LeafPageMovePlan leafPageMovePlan) {
        this.leafPageMovePlan = leafPageMovePlan;
    }

    @Override
    public Object getValue(int index) {
        return values[index];
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length);
        values = values.clone();
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    @Override
    BTreeLeafPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;
        Object[] aValues = new Object[a];
        Object[] bValues = new Object[b];
        System.arraycopy(values, 0, aValues, 0, a);
        System.arraycopy(values, a, bValues, 0, b);
        values = aValues;
        totalCount = a;
        BTreeLeafPage newPage = create(map, bKeys, bValues, bKeys.length, 0);
        newPage.replicationHostIds = replicationHostIds;
        recalculateMemory();
        return newPage;
    }

    @Override
    public long getTotalCount() {
        if (ASSERT) {
            long check = keys.length;
            if (check != totalCount) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Expected: {0} got: {1}", check,
                        totalCount);
            }
        }
        return totalCount;
    }

    @Override
    public void insertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        keys = newKeys;
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        values = newValues;
        keys[index] = key;
        values[index] = value;
        totalCount++;
        addMemory(map.getKeyType().getMemory(key) + map.getValueType().getMemory(value));
    }

    @Override
    public void remove(int index) {
        int keyLength = keys.length;
        super.remove(index);
        Object old = values[index];
        addMemory(-map.getValueType().getMemory(old));
        Object[] newValues = new Object[keyLength - 1];
        DataUtils.copyExcept(values, newValues, keyLength, index);
        values = newValues;
        totalCount--;
    }

    @Override
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);

        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();

        ByteBuffer oldBuff = buff;
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        map.getValueType().read(buff, values, keyLength);
        totalCount = keyLength;
        replicationHostIds = readReplicationHostIds(buff);
        recalculateMemory();
        oldBuff.limit(oldLimit);
    }

    @Override
    void writeLeaf(DataBuffer buff, boolean remote) {
        buff.put((byte) PageUtils.PAGE_TYPE_LEAF);
        writeReplicationHostIds(replicationHostIds, buff);
        buff.put((byte) (remote ? 1 : 0));
        if (!remote) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = map.getValueType();
            buff.putInt(keys.length);
            for (int i = 0, len = keys.length; i < len; i++) {
                kt.write(buff, keys[i]);
                vt.write(buff, values[i]);
            }
        }
    }

    static BTreeLeafPage readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        List<String> replicationHostIds = readReplicationHostIds(page);
        boolean remote = page.get() == 1;
        BTreeLeafPage p;

        if (remote) {
            p = BTreeLeafPage.createEmpty(map);
        } else {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = map.getValueType();
            int length = page.getInt();
            Object[] keys = new Object[length];
            Object[] values = new Object[length];
            for (int i = 0; i < length; i++) {
                keys[i] = kt.read(page);
                values[i] = vt.read(page);
            }
            p = BTreeLeafPage.create(map, keys, values, length, 0);
        }

        p.replicationHostIds = replicationHostIds;
        return p;
    }

    @Override
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength

        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        map.getValueType().write(buff, values, keyLength);
        writeReplicationHostIds(replicationHostIds, buff);

        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        if (replicatePage) {
            return typePos + 1;
        }

        updateChunkAndCachePage(chunk, start, pageLength, type);

        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.getBTreeStorage().removePage(pos, memory);
        }
        return typePos + 1;
    }

    @Override
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        write(chunk, buff, false);
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        StorageDataType valueType = map.getValueType();
        for (int i = 0, len = keys.length; i < len; i++) {
            mem += valueType.getMemory(values[i]);
        }
        addMemory(mem - memory);
    }

    @Override
    public BTreeLeafPage copy() {
        return copy(true);
    }

    private BTreeLeafPage copy(boolean removePage) {
        BTreeLeafPage newPage = create(map, keys, values, totalCount, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.replicationHostIds = replicationHostIds;
        newPage.leafPageMovePlan = leafPageMovePlan;
        if (removePage) {
            // mark the old as deleted
            removePage();
        }
        return newPage;
    }

    @Override
    void removeAllRecursive() {
        removePage();
    }

    /**
     * Create a new, empty page.
     * 
     * @param map the map
     * @return the new page
     */
    static BTreeLeafPage createEmpty(BTreeMap<?, ?> map) {
        return create(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY, 0, PageUtils.PAGE_MEMORY);
    }

    static BTreeLeafPage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, long totalCount, int memory) {
        BTreeLeafPage p = new BTreeLeafPage(map);
        // the position is 0
        p.keys = keys;
        p.values = values;
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    @Override
    void moveAllLocalLeafPages(String[] oldEndpoints, String[] newEndpoints) {
        Set<NetEndpoint> candidateEndpoints = BTreeMap.getCandidateEndpoints(map.db, newEndpoints);
        map.replicateOrMovePage(null, null, this, 0, oldEndpoints, false, candidateEndpoints);
    }

    @Override
    void replicatePage(DataBuffer buff, NetEndpoint localEndpoint) {
        BTreeLeafPage p = copy(false);
        BTreeChunk chunk = new BTreeChunk(0);
        buff.put((byte) PageUtils.PAGE_TYPE_LEAF);
        p.write(chunk, buff, true);
    }

    @Override
    protected void toString0(StringBuilder buff) {
        for (int i = 0, len = keys.length; i <= len; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (i < len) {
                buff.append(keys[i]);
                if (values != null) {
                    buff.append(':');
                    buff.append(values[i]);
                }
            }
        }
    }

    @Override
    protected void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info) {
        buff.append(indent).append("values: ");
        for (int i = 0, len = keys.length; i < len; i++) {
            if (i > 0)
                buff.append(", ");
            buff.append(values[i]);
        }
        buff.append('\n');
    }
}
