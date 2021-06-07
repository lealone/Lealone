/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import org.lealone.net.NetNode;

/**
 * Interface on which interested parties can be notified of high level node
 * state changes.
 *
 * Note that while INodeStateChangeSubscriber notify about gossip related
 * changes (INodeStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
public interface INodeLifecycleSubscriber {
    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param node the newly added node.
     */
    public void onJoinCluster(NetNode node);

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param node the node that is leaving.
     */
    public void onLeaveCluster(NetNode node);

    /**
     * Called when a node is marked UP.
     *
     * @param node the node marked UP.
     */
    public void onUp(NetNode node);

    /**
     * Called when a node is marked DOWN.
     *
     * @param node the node marked DOWN.
     */
    public void onDown(NetNode node);

    /**
     * Called when a node has moved (to a new token).
     *
     * @param node the node that has moved.
     */
    public void onMove(NetNode node);
}
