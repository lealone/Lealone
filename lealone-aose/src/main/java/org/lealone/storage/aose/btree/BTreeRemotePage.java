/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.db.DataBuffer;

public class BTreeRemotePage extends BTreePage {

    private List<String> replicationHostIds;

    BTreeRemotePage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isRemote() {
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
    void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength, boolean disableCheck) {
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);
        buff.get(); // type;
        replicationHostIds = readReplicationHostIds(buff);
    }

    @Override
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        write(chunk, buff, false);
    }

    private void write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_REMOTE;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0);
        buff.put((byte) type);
        writeReplicationHostIds(replicationHostIds, buff);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        if (replicatePage) {
            chunk.pagePositionToLengthMap.put(0L, pageLength);
        } else {
            updateChunkAndCachePage(chunk, start, pageLength, type);
        }
    }

    @Override
    void writeLeaf(DataBuffer buff, boolean remote) {
        buff.put((byte) PageUtils.PAGE_TYPE_REMOTE);
        writeReplicationHostIds(replicationHostIds, buff);
    }

    static BTreePage readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        List<String> replicationHostIds = readReplicationHostIds(page);
        BTreeRemotePage p = new BTreeRemotePage(map);
        p.replicationHostIds = replicationHostIds;
        return p;
    }

    @Override
    public BTreeRemotePage copy() {
        return copy(true);
    }

    private BTreeRemotePage copy(boolean removePage) {
        BTreeRemotePage newPage = new BTreeRemotePage(map);
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

    @Override
    public void removePage() {
        map.btreeStorage.removePage(pos, 0);
    }

    @Override
    void replicatePage(DataBuffer buff) {
        BTreeRemotePage p = copy(false);
        BTreeChunk chunk = new BTreeChunk(0);
        buff.put((byte) PageUtils.PAGE_TYPE_REMOTE);
        int start = buff.position();
        buff.putInt(0); // 回填pageLength
        p.write(chunk, buff, true);
        int pageLength = chunk.pagePositionToLengthMap.get(0L);
        buff.putInt(start, pageLength);
    }
}
