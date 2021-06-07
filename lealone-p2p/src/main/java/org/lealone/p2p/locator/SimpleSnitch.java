/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.List;

import org.lealone.net.NetNode;

/**
 * A simple node snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single node, which improves
 * cache locality.
 */
public class SimpleSnitch extends AbstractNodeSnitch {
    @Override
    public String getRack(NetNode node) {
        return "rack1";
    }

    @Override
    public String getDatacenter(NetNode node) {
        return "datacenter1";
    }

    @Override
    public void sortByProximity(final NetNode address, List<NetNode> addresses) {
        // Optimization to avoid walking the list
    }

    @Override
    public int compareNodes(NetNode target, NetNode a1, NetNode a2) {
        // Making all nodes equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }
}
