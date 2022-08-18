/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.db.DataBuffer;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.chunk.Chunk;

public class RemotePage extends Page {

    private List<String> replicationHostIds;

    public RemotePage(BTreeMap<?, ?> map) {
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
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength, boolean disableCheck) {
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);
        buff.get(); // type;
        replicationHostIds = readReplicationHostIds(buff);
    }

    @Override
    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        write(chunk, buff, false);
    }

    private void write(Chunk chunk, DataBuffer buff, boolean replicatePage) {
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
    public void writeLeaf(DataBuffer buff, boolean remote) {
        buff.put((byte) PageUtils.PAGE_TYPE_REMOTE);
        writeReplicationHostIds(replicationHostIds, buff);
    }

    public static Page readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        List<String> replicationHostIds = readReplicationHostIds(page);
        RemotePage p = new RemotePage(map);
        p.replicationHostIds = replicationHostIds;
        return p;
    }

    @Override
    public RemotePage copy() {
        return copy(true);
    }

    private RemotePage copy(boolean removePage) {
        RemotePage newPage = new RemotePage(map);
        if (removePage) {
            // mark the old as deleted
            removePage();
        }
        return newPage;
    }

    @Override
    public void removeAllRecursive() {
        removePage();
    }

    @Override
    public void removePage() {
        map.getBTreeStorage().removePage(pos, 0);
    }
}
