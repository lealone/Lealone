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

//只有key的场景，比如用来存索引的数据，索引的key就是由索引字段和rowKey组成
public class KeyPage extends LeafPage {

    public KeyPage(BTreeMap<?, ?> map) {
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
    KeyPage split(int at) { // 小于split key的放在左边，大于等于split key放在右边
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a, bKeys, 0, b);
        keys = aKeys;

        KeyPage newPage = create(map, bKeys, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public Page copyAndInsertLeaf(int index, Object key, Object value) {
        int len = keys.length + 1;
        Object[] newKeys = new Object[len];
        DataUtils.copyWithGap(keys, newKeys, len - 1, index);

        newKeys[index] = key;
        KeyPage p = copy(newKeys);
        p.addMemory(map.getKeyType().getMemory(key));
        map.incrementSize();// 累加全局计数器
        return p;
    }

    @Override
    public void remove(int index) {
        super.remove(index);
        map.decrementSize(); // 递减全局计数器
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
        // 兼容老版本
        for (int i = 0; i < keyLength; i++) {
            buff.get();
            buff.get();
            buff.get();
        }
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
        // 兼容老版本
        for (int i = 0; i < keyLength; i++) {
            buff.put((byte) 0);
            buff.put((byte) 0);
            buff.put((byte) 0);
        }
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
    public KeyPage copy() {
        return copy(keys);
    }

    private KeyPage copy(Object[] keys) {
        KeyPage newPage = create(map, keys, getMemory());
        super.copy(newPage);
        return newPage;
    }

    public static KeyPage createEmpty(BTreeMap<?, ?> map) {
        return createEmpty(map, true);
    }

    public static KeyPage createEmpty(BTreeMap<?, ?> map, boolean addToUsedMemory) {
        // 创建empty leaf page需要记录UsedMemory
        if (addToUsedMemory)
            map.getBTreeStorage().getBTreeGC().addUsedMemory(PageUtils.PAGE_MEMORY);
        return create(map, new Object[0], PageUtils.PAGE_MEMORY);
    }

    private static KeyPage create(BTreeMap<?, ?> map, Object[] keys, int memory) {
        KeyPage p = new KeyPage(map);
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
