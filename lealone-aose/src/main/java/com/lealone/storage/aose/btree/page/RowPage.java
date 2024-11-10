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

public class RowPage extends LeafPage {

    public RowPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object getValue(int index) {
        return keys[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        return keys[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        return keys[index];
    }

    @Override
    public Object setValue(int index, Object value) {
        Object old = keys[index];
        StorageDataType valueType = map.getValueType();
        addMemory(valueType.getMemory(value) - valueType.getMemory(old));
        keys[index] = value;
        return old;
    }

    @Override
    public Object getSplitKey(int index) {
        return map.getKeyType().getSplitKey(getKey(index));
    }

    @Override
    RowPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;

        RowPage newPage = create(map, bKeys, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);

        newKeys[index] = value;
        RowPage p = copy(newKeys);
        p.addMemory(map.getValueType().getMemory(value));
        map.incrementSize();// 累加全局计数器
        return p;
    }

    @Override
    public void remove(int index) {
        super.remove(index);
        map.decrementSize(); // 递减全局计数器
    }

    @Override
    protected int getKeyMemory(Object old) {
        // 也用值的类型来计算
        return map.getValueType().getMemory(old);
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
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
        map.getValueType().read(buff, keys, keyLength);
        buff.getInt(); // replicationHostIds
        recalculateMemory();
    }

    @Override
    public long writeUnsavedRecursive(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
        beforeWrite(pInfoOld);
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
        map.getValueType().write(buff, keys, keyLength);
        buff.putInt(0); // replicationHostIds

        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type);
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        addMemory(mem - memory, false);
    }

    @Override
    public RowPage copy() {
        return copy(keys);
    }

    private RowPage copy(Object[] keys) {
        RowPage newPage = create(map, keys, getMemory());
        super.copy(newPage);
        return newPage;
    }

    public static RowPage createEmpty(BTreeMap<?, ?> map) {
        return createEmpty(map, true);
    }

    public static RowPage createEmpty(BTreeMap<?, ?> map, boolean addToUsedMemory) {
        // 创建empty leaf page需要记录UsedMemory
        if (addToUsedMemory)
            map.getBTreeStorage().getBTreeGC().addUsedMemory(PageUtils.PAGE_MEMORY);
        return create(map, new Object[0], PageUtils.PAGE_MEMORY);
    }

    private static RowPage create(BTreeMap<?, ?> map, Object[] keys, int memory) {
        RowPage p = new RowPage(map);
        // the position is 0
        p.keys = keys;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory, false);
        }
        return p;
    }
}
