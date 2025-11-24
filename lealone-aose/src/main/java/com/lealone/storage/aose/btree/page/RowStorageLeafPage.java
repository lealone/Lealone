/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.chunk.Chunk;

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

        // 删除null记录
        ArrayList<Integer> deletedIndexs = new ArrayList<>(1);
        for (int i = 0; i < keyLength; i++) {
            Object v = getValue(i);
            if ((v == null) || ((v instanceof Lockable) && ((Lockable) v).getLockedValue() == null)) {
                deletedIndexs.add(i);
            }
        }
        if (!deletedIndexs.isEmpty()) {
            for (int index : deletedIndexs) {
                remove(index);
            }
        }
        return metaVersion;
    }

    protected abstract void writeValues(DataBuffer buff, int keyLength, int formatVersion);

    @Override
    public long write(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
        beforeWrite(pInfoOld);
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
        writeValues(buff, keyLength, chunk.formatVersion);
        buff.putInt(0); // replicationHostIds
        if (chunk.isNewFormatVersion())
            buff.putVarInt(pInfoOld.metaVersion);
        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type, true);
    }

    // 重写所有的RowStorageLeafPage，只需要修改CheckValue即可
    public static long rewrite(Chunk chunk, DataBuffer buff, ByteBuffer pageBuff, int pageLength) {
        return LeafPage.rewrite(chunk, buff, pageBuff, pageLength, 5, PageUtils.PAGE_TYPE_LEAF);
    }
}
