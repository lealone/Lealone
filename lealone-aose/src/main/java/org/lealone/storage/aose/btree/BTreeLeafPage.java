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
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.type.StorageDataType;

public class BTreeLeafPage extends BTreeLocalPage {

    /**
     * The values.
     * <p>
     * The array might be larger than needed, to avoid frequent re-sizing.
     */
    private Object[] values;

    private List<String> replicationHostIds;
    private LeafPageMovePlan leafPageMovePlan;
    private ColumnPageReference[] columnPages;

    static class ColumnPageReference {
        BTreeColumnPage page;
        long pos;

        public ColumnPageReference(long pos) {
            this.pos = pos;
        }
    }

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
    public Object getValue(int index, int columnIndex) {
        if (columnPages != null && columnPages[columnIndex].page == null) {
            readColumnPage(columnIndex);
        }
        return values[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        if (columnPages != null && columnIndexes != null) {
            // TODO 考虑一次加载连续的columnPage
            for (int columnIndex : columnIndexes) {
                if (columnPages[columnIndex].page == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        if (allColumns && columnPages != null) {
            // TODO 考虑一次加载连续的columnPage
            for (int columnIndex = 0, len = columnPages.length; columnIndex < len; columnIndex++) {
                if (columnPages[columnIndex].page == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = values[index];
        // this is slightly slower:
        // values = Arrays.copyOf(values, values.length);
        // values = values.clone(); // 在我的电脑上实测，copyOf实际上要比clone快一点点
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
        totalCount.set(a);
        BTreeLeafPage newPage = create(map, bKeys, bValues, new AtomicLong(bKeys.length), 0);
        newPage.replicationHostIds = replicationHostIds;
        recalculateMemory();
        return newPage;

        // int a = at, b = size - a;
        // // Object[] aKeys = new Object[a];
        // Object[] bKeys = new Object[b + initLength]; // 预分配空间
        // // System.arraycopy(keys, 0, aKeys, 0, a);
        // System.arraycopy(keys, a, bKeys, 0, b);
        // // keys = aKeys;
        // // Object[] aValues = new Object[a];
        // Object[] bValues = new Object[b + initLength];
        // // System.arraycopy(values, 0, aValues, 0, a);
        // System.arraycopy(values, a, bValues, 0, b);
        // // values = aValues;
        // totalCount = a;
        // size = a;
        // BTreeLeafPage newPage = create(map, bKeys, bValues, b, 0);
        // newPage.replicationHostIds = replicationHostIds;
        // newPage.length = bValues.length;
        // recalculateMemory();
        // return newPage;
    }

    static BTreeLeafPage create(BTreeMap<?, ?> map, ConcurrentSkipListMap<Object, Object> hashMap,
            AtomicLong totalCount, int memory) {
        BTreeLeafPage p = new BTreeLeafPage(map);
        // the position is 0
        p.totalCount = totalCount;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    @Override
    public long getTotalCount() {
        if (ASSERT) {
            long check = keys.length;
            if (check != totalCount.get()) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Expected: {0} got: {1}", check,
                        totalCount.get());
            }
        }
        return totalCount.get();
    }

    // 这里的实现虽然会copy数组，但并不是影响性能的地方，因为数组通常较小，System.arraycopy的性能较快，
    // 给数组预分配额外的空间能提升的性能并不大，已经测过
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
        totalCount.incrementAndGet();
        addMemory(map.getKeyType().getMemory(key) + map.getValueType().getMemory(value));
    }

    @Override
    boolean needSplit() {
        return memory > map.btreeStorage.getPageSplitSize() && keys.length > 1;
    }

    @Override
    public int getKeyCount() {
        return keys.length;
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
        totalCount.decrementAndGet();
    }

    void readRowStorage(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);
        buff.get(); // mode
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
        totalCount.set(keyLength);
        replicationHostIds = readReplicationHostIds(buff);
        recalculateMemory();
        oldBuff.limit(oldLimit);
    }

    void readColumnStorageSinglePage(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);
        buff.get(); // mode
        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        int columnCount = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        for (int i = 0; i < columnCount; i++) {
            buff.getLong();
        }

        ByteBuffer oldBuff = buff;
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        StorageDataType valueType = map.getValueType();
        for (int row = 0; row < keyLength; row++) {
            values[row] = valueType.readMeta(buff, columnCount);
        }
        replicationHostIds = readReplicationHostIds(buff);
        for (int col = 0; col < columnCount; col++) {
            int columnPageStartPos = buff.position();
            int columnPageLength = buff.getInt();
            int columnPageType = buff.get();
            ByteBuffer columnPageBuff = expandPage(buff, columnPageType, columnPageStartPos, columnPageLength);
            for (int row = 0; row < keyLength; row++) {
                valueType.readColumn(columnPageBuff, values[row], col);
            }
        }
        totalCount.set(keyLength);
        recalculateMemory();
        oldBuff.limit(oldLimit);
    }

    void readColumnStorageMultiPages(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);
        buff.get(); // mode
        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        int columnCount = DataUtils.readVarInt(buff);
        columnPages = new ColumnPageReference[columnCount];
        keys = new Object[keyLength];
        int type = buff.get();
        for (int i = 0; i < columnCount; i++) {
            long pos = buff.getLong();
            columnPages[i] = new ColumnPageReference(pos);
        }

        ByteBuffer oldBuff = buff;
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        StorageDataType valueType = map.getValueType();
        for (int row = 0; row < keyLength; row++) {
            values[row] = valueType.readMeta(buff, columnCount);
        }
        // for (int i = 0; i < columnCount; i++) {
        // columnPages[i].page = new BTreeColumnPage(map, values, i);
        // }
        replicationHostIds = readReplicationHostIds(buff);
        // 延迟加载
        // for (int col = 0; col < columnCount; col++) {
        // BTreeColumnPage page = (BTreeColumnPage) map.btreeStorage.readPage(columnPages[col].pos);
        // if (page.values == null) {
        // columnPages[col].page = page;
        // page.readColumnPage(values, col);
        // map.btreeStorage.cachePage(columnPages[col].pos, page, page.getMemory());
        // } else {
        // // 有可能因为缓存紧张，导致keys所在的page被逐出了，但是列所在的某些page还在
        // values = page.values;
        // }
        // }
        totalCount.set(keyLength);
        recalculateMemory();
        oldBuff.limit(oldLimit);
    }

    @Override
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int mode = buff.get(buff.position() + 4);
        switch (PageStorageMode.values()[mode]) {
        case COLUMN_STORAGE:
            readColumnStorageMultiPages(buff, chunkId, offset, maxLength, disableCheck);
            break;
        case COLUMN_STORAGE_SINGLE_PAGE:
            readColumnStorageSinglePage(buff, chunkId, offset, maxLength, disableCheck);
            break;
        default:
            readRowStorage(buff, chunkId, offset, maxLength, disableCheck);
        }
    }

    void readColumnPage(int columnIndex) {
        BTreeColumnPage page = (BTreeColumnPage) map.btreeStorage.readPage(columnPages[columnIndex].pos);
        if (page.values == null) {
            columnPages[columnIndex].page = page;
            page.readColumnPage(values, columnIndex);
            map.btreeStorage.cachePage(columnPages[columnIndex].pos, page, page.getMemory());
        } else {
            // 有可能因为缓存紧张，导致keys所在的page被逐出了，但是列所在的某些page还在
            values = page.values;
        }
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
            p = BTreeLeafPage.create(map, keys, values, new AtomicLong(length), 0);
        }

        p.replicationHostIds = replicationHostIds;
        return p;
    }

    int writeRowStorage(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) map.pageStorageMode.ordinal());
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
        // compressStart = start;
        // compressPage(buff, compressStart, type, typePos);
        // pageLength = buff.position() - start;
        // compressStart = start;
        // compressPage(buff, compressStart, type, typePos);
        // pageLength = buff.position() - start;
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
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        switch (map.pageStorageMode) {
        case COLUMN_STORAGE:
            return writeColumnStorageMultiPages(chunk, buff, replicatePage);
        case COLUMN_STORAGE_SINGLE_PAGE:
            return writeColumnStorageSinglePage(chunk, buff, replicatePage);
        default:
            return writeRowStorage(chunk, buff, replicatePage);
        }
    }

    int writeColumnStorageSinglePage(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) map.pageStorageMode.ordinal());
        StorageDataType valueType = map.getValueType();
        int columnCount = valueType.getColumnCount();
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength).putVarInt(columnCount);
        int typePos = buff.position();
        buff.put((byte) type);
        int columnPageStartPos = buff.position();
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(0);
        }
        // int compressStart0 = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        // valueType.write(buff, values, keyLength, 0);
        for (int row = 0; row < keyLength; row++) {
            valueType.writeMeta(buff, values[row]);
        }
        writeReplicationHostIds(replicationHostIds, buff);
        int compressStart = buff.position();
        int[] posArray = new int[columnCount];
        for (int col = 0; col < columnCount; col++) {
            posArray[col] = buff.position();
            int columnPagPos = buff.position();
            int columnPageType = PageUtils.PAGE_TYPE_LEAF;
            buff.putInt(0); // 回填pageLength
            int columnPageTypePos = buff.position();
            buff.put((byte) columnPageType);
            compressStart = buff.position();
            for (int row = 0; row < keyLength; row++) {
                valueType.writeColumn(buff, values[row], col);
            }
            compressPage(buff, compressStart, columnPageType, columnPageTypePos);
            int pageLength = buff.position() - columnPagPos;
            buff.putInt(columnPagPos, pageLength);
        }
        // map.getValueType().write(buff, values, keyLength, 1);
        // compressPage(buff, compressStart, type, typePos);

        // int[] posArray = map.getValueType().write(buff, values, keyLength, 1);
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

        // compressPage(buff, compressStart0, type, typePos);
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

    int writeColumnStorageMultiPages(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) map.pageStorageMode.ordinal());
        StorageDataType valueType = map.getValueType();
        int columnCount = valueType.getColumnCount();
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength).putVarInt(columnCount);
        int typePos = buff.position();
        buff.put((byte) type);
        int columnPageStartPos = buff.position();
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(0);
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        for (int row = 0; row < keyLength; row++) {
            valueType.writeMeta(buff, values[row]);
        }
        writeReplicationHostIds(replicationHostIds, buff);
        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        long[] posArray = new long[columnCount];
        for (int col = 0; col < columnCount; col++) {
            BTreeColumnPage page = new BTreeColumnPage(map, values, col);
            posArray[col] = page.writeColumnPage(chunk, buff, replicatePage);
        }
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

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

    // @Override
    // protected int recalculateKeysMemory() {
    // int mem = PageUtils.PAGE_MEMORY;
    // StorageDataType keyType = map.getKeyType();
    // for (int i = 0; i < size; i++) {
    // mem += keyType.getMemory(keys[i]);
    // }
    // return mem;
    // }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        StorageDataType valueType = map.getValueType();
        for (int i = 0; i < keys.length; i++) {
            mem += valueType.getMemory(values[i]);
        }
        addMemory(mem - memory);
    }

    // @Override
    // protected void recalculateMemory() {
    // StorageDataType keyType = map.getKeyType();
    // StorageDataType valueType = map.getValueType();
    // int mem = PageUtils.PAGE_MEMORY;
    // for (Entry<Object, Object> e : hashMap.entrySet()) {
    // mem += keyType.getMemory(e.getKey());
    // mem += valueType.getMemory(e.getValue());
    // }
    // addMemory(mem - memory);
    // }

    @Override
    public BTreeLeafPage copy() {
        return copy(true);
    }

    private BTreeLeafPage copy(boolean removePage) {
        BTreeLeafPage newPage = create(map, keys, values, new AtomicLong(totalCount.get()), getMemory());
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
        return create(map, EMPTY_OBJECT_ARRAY, EMPTY_OBJECT_ARRAY, new AtomicLong(0), PageUtils.PAGE_MEMORY);
    }

    static BTreeLeafPage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, AtomicLong totalCount, int memory) {
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

    @Override
    public Object[] getValues() {
        return values;
    }
}
