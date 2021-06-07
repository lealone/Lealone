/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.HashSet;
import java.util.Set;

import org.lealone.db.IDatabase;

public class LocalNetNodeManager implements NetNodeManager {

    private static final LocalNetNodeManager INSTANCE = new LocalNetNodeManager();

    public static LocalNetNodeManager getInstance() {
        return INSTANCE;
    }

    protected LocalNetNodeManager() {
    }

    @Override
    public Set<NetNode> getLiveNodes() {
        HashSet<NetNode> set = new HashSet<>(1);
        set.add(NetNode.getLocalP2pNode());
        return set;
    }

    @Override
    public String[] assignNodes(IDatabase db) {
        return new String[] { NetNode.getLocalTcpHostAndPort() };
    }

    @Override
    public boolean isLocal() {
        return true;
    }
}
