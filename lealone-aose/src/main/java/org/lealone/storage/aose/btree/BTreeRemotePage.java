/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.db.DataBuffer;
import org.lealone.net.NetNode;

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
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);

        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

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
        map.btreeStorage.removePage(pos, 0);
    }

    @Override
    void replicatePage(DataBuffer buff, NetNode localNode) {
        BTreeRemotePage p = copy(false);
        BTreeChunk chunk = new BTreeChunk(0);
        buff.put((byte) PageUtils.PAGE_TYPE_REMOTE);
        p.write(chunk, buff, true);
    }
}
