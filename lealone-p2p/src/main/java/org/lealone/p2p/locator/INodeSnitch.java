/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.Collection;
import java.util.List;

import org.lealone.net.NetNode;

/**
 * This interface helps determine location of node in the data center relative to another node.
 * Give a node A and another node B it can tell if A and B are on the same rack or in the same
 * data center.
 */
public interface INodeSnitch {
    /**
     * returns a String repesenting the rack this node belongs to
     */
    public String getRack(NetNode node);

    /**
     * returns a String representing the datacenter this node belongs to
     */
    public String getDatacenter(NetNode node);

    /**
     * returns a new <tt>List</tt> sorted by proximity to the given node
     */
    public List<NetNode> getSortedListByProximity(NetNode address, Collection<NetNode> unsortedAddress);

    /**
     * This method will sort the <tt>List</tt> by proximity to the given address.
     */
    public void sortByProximity(NetNode address, List<NetNode> addresses);

    /**
     * compares two nodes in relation to the target node, returning as Comparator.compare would
     */
    public int compareNodes(NetNode target, NetNode a1, NetNode a2);

    /**
     * called after Gossiper instance exists immediately before it starts gossiping
     */
    public void gossiperStarting();

    /**
     * Returns whether for a range query doing a query against merged is likely
     * to be faster than 2 sequential queries, one against l1 followed by one against l2.
     */
    public boolean isWorthMergingForRangeQuery(List<NetNode> merged, List<NetNode> l1, List<NetNode> l2);
}
