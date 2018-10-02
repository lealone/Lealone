/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import org.lealone.db.DataBuffer;
import org.lealone.net.NetEndpoint;

/**
 * A remote page.
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeRemotePage extends BTreePage {

    private List<String> replicationHostIds;

    BTreeRemotePage(BTreeMap<?, ?> map) {
        super(map);
        pos = -1;
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
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean isLeaf) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);

        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength);

        buff.get(); // type;

        replicationHostIds = readReplicationHostIds(buff);
        buff.limit(oldLimit);
    }

    @Override
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int type = PageUtils.PAGE_TYPE_REMOTE;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0);
        int typePos = buff.position();
        buff.put((byte) type);
        writeReplicationHostIds(replicationHostIds, buff);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        if (replicatePage) {
            return typePos + 1;
        }

        updateChunkAndCachePage(chunk, start, pageLength, type);
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
        map.storage.removePage(pos, 0);
    }

    @Override
    void moveAllLocalLeafPages(String[] oldEndpoints, String[] newEndpoints) {
        Set<NetEndpoint> candidateEndpoints = BTreeMap.getCandidateEndpoints(map.db, newEndpoints);
        map.replicateOrMovePage(null, null, this, 0, oldEndpoints, false, candidateEndpoints);
    }

    @Override
    void replicatePage(DataBuffer buff, NetEndpoint localEndpoint) {
        BTreeRemotePage p = copy(false);
        BTreeChunk chunk = new BTreeChunk(0);
        buff.put((byte) PageUtils.PAGE_TYPE_REMOTE);
        p.write(chunk, buff, true);
    }
}
