/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.db.Session;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.PageKey;
import org.lealone.storage.StorageCommand;
import org.lealone.storage.replication.ReplicationSession;

/**
 * A pointer to a page, either in-memory or using a page position.
 * 
 * @author H2 Group
 * @author zhh
 */
public class PageReference {

    private static final long REMOTE_PAGE_POS = -1;

    static PageReference createRemotePageReference(Object key, boolean first) {
        return new PageReference(null, REMOTE_PAGE_POS, 0, key, first);
    }

    static PageReference createRemotePageReference() {
        return new PageReference(null, REMOTE_PAGE_POS, 0);
    }

    /**
     * The page, if in memory, or null.
     */
    BTreePage page;

    PageKey pageKey;

    /**
     * The position, if known, or 0.
     */
    long pos;

    /**
     * The descendant count for this child page.
     */
    final long count;

    List<String> replicationHostIds;

    public PageReference(BTreePage page, long pos, long count) {
        this.page = page;
        this.pos = pos;
        this.count = count;
        if (page != null) {
            replicationHostIds = page.getReplicationHostIds();
        }
    }

    public PageReference(BTreePage page, long pos, long count, Object key, boolean first) {
        this(page, pos, count);
        setPageKey(key, first);
    }

    @Override
    public String toString() {
        return "PageReference[ pos=" + pos + ", count=" + count + " ]";
    }

    void setPageKey(Object key, boolean first) {
        pageKey = new PageKey(key, first);
    }

    boolean isRemotePage() {
        return pos == REMOTE_PAGE_POS;
    }

    boolean isLeafPage() {
        return pos != REMOTE_PAGE_POS && replicationHostIds != null;
    }

    boolean isNodePage() {
        return pos != REMOTE_PAGE_POS && replicationHostIds == null;
    }

    synchronized BTreePage readRemotePage(BTreeMap<Object, Object> map) {
        if (page != null) {
            return page;
        }

        // TODO 支持多节点容错
        String remoteHostId = replicationHostIds.get(0);
        List<NetEndpoint> replicationEndpoints = BTreeMap.getReplicationEndpoints(map.db,
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
