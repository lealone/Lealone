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
import com.lealone.storage.aose.btree.BTreeStorage;
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

    protected abstract void readValues(ByteBuffer buff, int keyLength);

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
        readValues(buff, keyLength);
        buff.getInt(); // replicationHostIds
        recalculateMemory();
    }

    protected abstract void writeValues(DataBuffer buff, int keyLength);

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
        map.getKeyType().write(buff, keys, keyLength);
        writeValues(buff, keyLength);
        buff.putInt(0); // replicationHostIds

        compressPage(buff, compressStart, type, typePos);
        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);

        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunkAndPage(pInfoOld, chunk, start, pageLength, type, true);
    }

    public static long rewrite(BTreeStorage bs, Chunk chunk, DataBuffer buff, long pos) {
        Chunk c = bs.getChunkManager().getChunk(pos);
        long filePos = Chunk.getFilePos(PageUtils.getPageOffset(pos));
        int pageLength = c.getPageLength(pos);
        ByteBuffer pageBuff = c.fileStorage.readFully(filePos, pageLength);
        int start = buff.position();
        int checkPos = start + 5;
        buff.put(pageBuff);
        writeCheckValue(buff, chunk, start, pageLength, checkPos);

        return updateChunk(chunk, start, pageLength, PageUtils.PAGE_TYPE_LEAF);
    }
}
