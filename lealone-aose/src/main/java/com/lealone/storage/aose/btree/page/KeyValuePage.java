/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.type.StorageDataType;

public class KeyValuePage extends LeafPage {

    private Object[] values;
    private PageReference[] columnPages;
    private boolean isAllColumnPagesRead;

    public KeyValuePage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object getValue(int index) {
        return values[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        if (columnPages != null && columnIndexes != null && !isAllColumnPagesRead) {
            for (int columnIndex : columnIndexes) {
                readColumnPage(columnIndex);
            }
        }
        return values[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        if (columnPages != null && allColumns && !isAllColumnPagesRead) {
            readAllColumnPages();
        }
        return values[index];
    }

    private void readAllColumnPages() {
        for (int columnIndex = 0, len = columnPages.length; columnIndex < len; columnIndex++) {
            readColumnPage(columnIndex);
        }
        isAllColumnPagesRead = true;
    }

    private void readColumnPage(int columnIndex) {
        PageReference ref = columnPages[columnIndex];
        ColumnPage page = (ColumnPage) ref.getOrReadPage();
        if (page.getMemory() <= 0)
            page.readColumn(values, columnIndex);
    }

    private void markAllColumnPagesDirty() {
        if (columnPages != null) {
            if (!isAllColumnPagesRead) {
                readAllColumnPages();
            }
            for (PageReference ref : columnPages) {
                if (ref != null) {
                    Page p = ref.getPage();
                    if (p != null) {
                        p.markDirty();
                    }
                }
            }
            columnPages = null;
        }
    }

    @Override
    public void markDirty() {
        if (columnPages != null)
            markAllColumnPagesDirty();
        super.markDirty();
    }

    @Override
    public Object setValue(int index, Object value) {
        if (columnPages != null)
            markAllColumnPagesDirty();
        Object old = values[index];
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        values[index] = value;
        return old;
    }

    @Override
    KeyValuePage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
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

        KeyValuePage newPage = create(map, bKeys, bValues, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        if (columnPages != null)
            markAllColumnPagesDirty();
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);
        Object[] newValues = new Object[len];
        DataUtils.copyWithGap(values, newValues, len - 1, index);
        newKeys[index] = key;
        newValues[index] = value;
        KeyValuePage p = copy(newKeys, newValues);
        p.addMemory(map.getKeyType().getMemory(key) + map.getValueType().getMemory(value));
        map.incrementSize();// 累加全局计数器
        return p;
    }

    @Override
    public void remove(int index) {
        if (columnPages != null)
            markAllColumnPagesDirty();
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
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int mode = buff.get(buff.position() + 4);
        switch (PageStorageMode.values()[mode]) {
        case COLUMN_STORAGE:
            readColumnStorage(buff, chunkId, offset, expectedPageLength);
            break;
        default:
            readRowStorage(buff, chunkId, offset, expectedPageLength);
        }
    }

    private void readRowStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength);

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

    private void readColumnStorage(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength);

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

    @Override
    public long writeUnsavedRecursive(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
        beforeWrite(pInfoOld);
        switch (map.getPageStorageMode()) {
        case COLUMN_STORAGE:
            return writeColumnStorage(pInfoOld, chunk, buff);
        default:
            return writeRowStorage(pInfoOld, chunk, buff);
        }
    }

    private long writeRowStorage(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
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

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type);
    }

    private long writeColumnStorage(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
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
            ColumnPage page = new ColumnPage(map);
            page.setRef(new PageReference(map.getBTreeStorage(), 0));
            posArray[col] = page.write(chunk, buff, values, col);
        }
        int oldPos = buff.position();
        buff.position(columnPageStartPos);
        for (int i = 0; i < columnCount; i++) {
            buff.putLong(posArray[i]);
        }
        buff.position(oldPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type);
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
    public KeyValuePage copy() {
        return copy(keys, values);
    }

    private KeyValuePage copy(Object[] keys, Object[] values) {
        KeyValuePage newPage = create(map, keys, values, getMemory());
        super.copy(newPage);
        return newPage;
    }

    /**
     * Create a new, empty page.
     * 
     * @param map the map
     * @return the new page
     */
    public static KeyValuePage createEmpty(BTreeMap<?, ?> map) {
        return createEmpty(map, true);
    }

    public static KeyValuePage createEmpty(BTreeMap<?, ?> map, boolean addToUsedMemory) {
        // 创建empty leaf page需要记录UsedMemory
        if (addToUsedMemory)
            map.getBTreeStorage().getBTreeGC().addUsedMemory(PageUtils.PAGE_MEMORY);
        return create(map, new Object[0], new Object[0], PageUtils.PAGE_MEMORY);
    }

    private static KeyValuePage create(BTreeMap<?, ?> map, Object[] keys, Object[] values, int memory) {
        KeyValuePage p = new KeyValuePage(map);
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
