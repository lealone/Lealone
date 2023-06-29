/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.type.StorageDataType;

public class LeafPage extends LocalPage {

    private Object[] values;
    private PageReference[] columnPages;

    public LeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return values == null || values.length == 0;
    }

    @Override
    public Object getValue(int index) {
        return values[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        if (columnPages != null && columnIndexes != null) {
            for (int columnIndex : columnIndexes) {
                if (columnPages[columnIndex].getPage() == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        if (columnPages != null && allColumns) {
            for (int columnIndex = 0, len = columnPages.length; columnIndex < len; columnIndex++) {
                if (columnPages[columnIndex].getPage() == null) {
                    readColumnPage(columnIndex);
                }
            }
        }
        return values[index];
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = values[index];
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        values[index] = value;

        // 执行setValue并不会copy leaf page，所以可以释放Buff
        PageInfo pInfo = getRef().getPageInfo();
        if (pInfo.pos > 0) {
            map.getBTreeStorage().getBTreeGC().addUsedMemory(-pInfo.getBuffMemory());
            pInfo.releaseBuff();
            pInfo.pos = 0;
        }
        return old;
    }

    @Override
    LeafPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
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

        LeafPage newPage = create(map, bKeys, bValues, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        newKeys[index] = key;
        newValues[index] = value;
        LeafPage p = copy(newKeys, newValues);
        p.addMemory(map.getKeyType().getMemory(key) + map.getValueType().getMemory(value));
        map.incrementSize();// 累加全局计数器
        return p;
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
        map.decrementSize(); // 递减全局计数器
    }

    @Override
    public void read(PageInfo pInfo, ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        int mode = buff.get(buff.position() + 4);
        switch (PageStorageMode.values()[mode]) {
        case COLUMN_STORAGE:
            readColumnStorage(buff, chunkId, offset, expectedPageLength, disableCheck);
            break;
        default:
            readRowStorage(buff, chunkId, offset, expectedPageLength, disableCheck);
        }
    }

    private void readRowStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        map.getValueType().read(buff, values, keyLength);
        buff.getInt(); // replicationHostIds
        recalculateMemory();
    }

    private void readColumnStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        int columnCount = DataUtils.readVarInt(buff);
        columnPages = new PageReference[columnCount];
        keys = new Object[keyLength];
        int type = buff.get();
        for (int i = 0; i < columnCount; i++) {
            long pos = buff.getLong();
            columnPages[i] = new PageReference(map.getBTreeStorage(), pos);
        }
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        values = new Object[keyLength];
        StorageDataType valueType = map.getValueType();
        for (int row = 0; row < keyLength; row++) {
            values[row] = valueType.readMeta(buff, columnCount);
        }
        buff.getInt(); // replicationHostIds
        recalculateMemory();
        // 延迟加载列
    }

    private void readColumnPage(int columnIndex) {
        PageReference ref = columnPages[columnIndex];
        ColumnPage page = (ColumnPage) ref.getOrReadPage();
        if (page.values == null) {
            page.readColumn(ref.getPageInfo(), values, columnIndex);
            map.getBTreeStorage().gcIfNeeded(page.getTotalMemory());
        } else {
            // 有可能因为缓存紧张，导致keys所在的page被逐出了，但是列所在的某些page还在
            values = page.values;
        }
    }

    @Override
    public long writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        beforeWrite();
        switch (map.getPageStorageMode()) {
        case COLUMN_STORAGE:
            return writeColumnStorage(chunk, buff);
        default:
            return writeRowStorage(chunk, buff);
        }
    }

    private long writeRowStorage(Chunk chunk, DataBuffer buff) {
        PagePos oldPagePos = posRef.get();
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) map.getPageStorageMode().ordinal());
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);
        map.getValueType().write(buff, values, keyLength);
        buff.putInt(0); // replicationHostIds

        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunkAndPage(oldPagePos, chunk, start, pageLength, type);
    }

    private long writeColumnStorage(Chunk chunk, DataBuffer buff) {
        PagePos oldPagePos = posRef.get();
        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) map.getPageStorageMode().ordinal());
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
        buff.putInt(0); // replicationHostIds
        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        long[] posArray = new long[columnCount];
        for (int col = 0; col < columnCount; col++) {
            ColumnPage page = new ColumnPage(map, values, col);
            posArray[col] = page.write(chunk, buff);
        }
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

        return updateChunkAndPage(oldPagePos, chunk, start, pageLength, type);
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        StorageDataType valueType = map.getValueType();
        for (int i = 0; i < keys.length; i++) {
            mem += valueType.getMemory(values[i]);
        }
        addMemory(mem - memory, false);
    }

    @Override
    public LeafPage copy() {
        return copy(keys, values);
    }

    private LeafPage copy(Object[] keys, Object[] values) {
        LeafPage newPage = create(map, keys, values, getMemory());
        super.copy(newPage);
        return newPage;
    }

    /**
     * Create a new, empty page.
     * 
     * @param map the map
     * @return the new page
     */
    public static LeafPage createEmpty(BTreeMap<?, ?> map) {
        // 创建empty leaf page需要记录UsedAndDirtyMemory
        map.getBTreeStorage().getBTreeGC().addUsedAndDirtyMemory(PageUtils.PAGE_MEMORY);
        return create(map, new Object[0], new Object[0], PageUtils.PAGE_MEMORY);
    }

    static LeafPage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, int memory) {
        LeafPage p = new LeafPage(map);
        // the position is 0
        p.keys = keys;
        p.values = values;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory, false);
        }
        return p;
    }
}
