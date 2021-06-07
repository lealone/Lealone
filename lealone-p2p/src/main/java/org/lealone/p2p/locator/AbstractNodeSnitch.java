/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;

public abstract class AbstractNodeSnitch implements INodeSnitch {
    @Override
    public abstract int compareNodes(NetNode target, NetNode a1, NetNode a2);

    /**
     * Sorts the <tt>Collection</tt> of node addresses by proximity to the given address
     * @param address the address to sort by proximity to
     * @param unsortedAddress the nodes to sort 
     * @return a new sorted <tt>List</tt>
     */
    @Override
    public List<NetNode> getSortedListByProximity(NetNode address, Collection<NetNode> unsortedAddress) {
        List<NetNode> preferred = new ArrayList<NetNode>(unsortedAddress);
        sortByProximity(address, preferred);
        return preferred;
    }

    /**
     * Sorts the <tt>List</tt> of node addresses, in-place, by proximity to the given address
     * @param address the address to sort the proximity by
     * @param addresses the nodes to sort
     */
    @Override
    public void sortByProximity(final NetNode address, List<NetNode> addresses) {
        Collections.sort(addresses, new Comparator<NetNode>() {
            @Override
            public int compare(NetNode a1, NetNode a2) {
                return compareNodes(address, a1, a2);
            }
        });
    }

    @Override
    public void gossiperStarting() {
        // noop by default
    }

    @Override
    public boolean isWorthMergingForRangeQuery(List<NetNode> merged, List<NetNode> l1, List<NetNode> l2) {
        // Querying remote DC is likely to be an order of magnitude slower than
        // querying locally, so 2 queries to local nodes is likely to still be
        // faster than 1 query involving remote ones
        boolean mergedHasRemote = hasRemoteNode(merged);
        return mergedHasRemote ? hasRemoteNode(l1) || hasRemoteNode(l2) : true;
    }

    private boolean hasRemoteNode(List<NetNode> l) {
        String localDc = ConfigDescriptor.getLocalDataCenter();
        for (NetNode ep : l) {
            if (!localDc.equals(getDatacenter(ep)))
                return true;
        }
        return false;
    }
}
