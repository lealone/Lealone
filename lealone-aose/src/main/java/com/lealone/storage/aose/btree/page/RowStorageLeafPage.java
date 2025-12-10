/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.type.StorageDataType;

public abstract class RowStorageLeafPage extends LeafPage {

    protected RowStorageLeafPage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public Object getValue(int index) {
        return getValues()[index];
    }

    @Override
    public Object getValue(int index, int[] columnIndexes) {
        return getValues()[index];
    }

    @Override
    public Object getValue(int index, boolean allColumns) {
        return getValues()[index];
    }

    protected abstract void readValues(ByteBuffer buff, int keyLength, int formatVersion);

    @Override
    public int read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        buff.get(); // mode
        readCheckValue(buff, chunkId, offset, pageLength);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        buff = expandPage(buff, type, start, pageLength);

        Chunk chunk = map.getBTreeStorage().getChunkManager().getChunk(chunkId);
        map.getKeyType().read(buff, keys, keyLength, chunk.formatVersion);
        readValues(buff, keyLength, chunk.formatVersion);
        buff.getInt(); // replicationHostIds
        int metaVersion = 0;
        if (chunk.isNewFormatVersion())
            metaVersion = DataUtils.readVarInt(buff); // metaVersion
        recalculateMemory();
        return metaVersion;
    }

    protected abstract void writeValues(Object[] values, DataBuffer buff, int keyLength,
            int formatVersion);

    @Override
    public long write(PageInfo pInfoOld, Chunk chunk, DataBuffer buff, AtomicBoolean isLocked) {
        beforeWrite(pInfoOld);
        Object[] keys;
        Object[] values;
        boolean isLockedPage = false;
        StorageDataType valueType = map.getValueType();
        if (valueType.isTransactional()) {
            Object[] objects = valueType.getCommittedObjects(this.keys, getValues());
            keys = (Object[]) objects[0];
            values = (Object[]) objects[1];
            isLockedPage = (Boolean) objects[2];
        } else {
            keys = this.keys;
            values = getValues();
        }
        if (isLockedPage)
            isLocked.set(true);

        int start = buff.position();
        int keyLength = keys.length;
        int type = PageUtils.PAGE_TYPE_LEAF;
        buff.putInt(0); // 回填pageLength
        buff.put((byte) PageStorageMode.ROW_STORAGE.ordinal());
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        buff.put((byte) type);
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength, chunk.formatVersion);
        writeValues(values, buff, keyLength, chunk.formatVersion);
        buff.putInt(0); // replicationHostIds
        if (chunk.isNewFormatVersion())
            buff.putVarInt(pInfoOld.metaVersion);
        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type, true, isLockedPage);
    }

    // 重写所有的RowStorageLeafPage，只需要修改CheckValue即可
    public static long rewrite(Chunk chunk, DataBuffer buff, ByteBuffer pageBuff, int pageLength) {
        return LeafPage.rewrite(chunk, buff, pageBuff, pageLength, 5, PageUtils.PAGE_TYPE_LEAF);
    }
}
