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
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.Session;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.replication.ReplicationSession;

public class PageReference {

    private static final long REMOTE_PAGE_POS = -1;

    static PageReference createRemotePageReference(Object key, boolean first) {
        return new PageReference(null, REMOTE_PAGE_POS, new AtomicLong(0), key, first);
    }

    static PageReference createRemotePageReference() {
        return new PageReference(null, REMOTE_PAGE_POS, new AtomicLong(0));
    }

    BTreePage page;
    PageKey pageKey;
    long pos;
    AtomicLong count;
    List<String> replicationHostIds;

    public PageReference(long pos, AtomicLong count) {
        this.pos = pos;
    }

    public PageReference(BTreePage page, long pos, AtomicLong count) {
        this.page = page;
        this.pos = pos;
        this.count = count;
        if (page != null) {
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    public PageReference(BTreePage page, long pos) {
        this.page = page;
        this.pos = pos;
        if (page != null) {
            replicationHostIds = page.getReplicationHostIds();
            count = page.getCounter();
        }
    }

    public PageReference(BTreePage page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            count = page.getCounter();
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    PageReference(BTreePage page, long pos, AtomicLong count, Object key, boolean first) {
        this(page, pos, count);
        setPageKey(key, first);
    }

    PageReference(BTreePage page, Object key, boolean first) {
        this(page);
        setPageKey(key, first);
    }

    public void replacePage(BTreePage page) {
        this.page = page;
        if (page != null) {
            pos = page.getPos();
            count = page.getCounter();
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    public void setCount(AtomicLong count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "PageReference[ pos=" + pos + ", count=" + count + " ]";
    }

    void setPageKey(Object key, boolean first) {
        pageKey = new PageKey(key, first);
    }

    boolean isRemotePage() {
        if (page != null)
            return page.isRemote();
        else
            return pos == REMOTE_PAGE_POS;
    }

    boolean isLeafPage() {
        if (page != null)
            return page.isLeaf();
        else
            return pos != REMOTE_PAGE_POS && PageUtils.isLeafPage(pos);
    }

    boolean isNodePage() {
        if (page != null)
            return page.isNode();
        else
            return pos != REMOTE_PAGE_POS && PageUtils.isNodePage(pos);
    }

    synchronized BTreePage readRemotePage(BTreeMap<Object, Object> map) {
        if (page != null) {
            return page;
        }

        // TODO 支持多节点容错
        String remoteHostId = replicationHostIds.get(0);
        List<NetEndpoint> replicationEndpoints = DistributedBTreeMap.getReplicationEndpoints(map.db,
                new String[] { remoteHostId });
        Session session = map.db.createInternalSession(true);
        ReplicationSession rs = map.db.createReplicationSession(session, replicationEndpoints);
        try (StorageCommand c = rs.createStorageCommand()) {
            ByteBuffer pageBuffer = c.readRemotePage(map.getName(), pageKey);
            page = BTreePage.readReplicatedPage(map, pageBuffer);
        }

        if (!map.isShardingMode() || (page.getReplicationHostIds() != null
                && page.getReplicationHostIds().contains(NetEndpoint.getLocalTcpHostAndPort()))) {
            pos = 0;
        }
        return page;
    }

    // test only
    public BTreePage getPage() {
        return page;
    }

    // test only
    public void setReplicationHostIds(List<String> replicationHostIds) {
        this.replicationHostIds = replicationHostIds;
    }
}
