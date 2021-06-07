/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import org.lealone.net.NetNode;

/**
 * Implemented by the Gossiper to convict an node
 * based on the PHI calculated by the Failure Detector on the inter-arrival
 * times of the heart beats.
 */
public interface IFailureDetectionEventListener {
    /**
     * Convict the specified node.
     *
     * @param ep  node to be convicted
     * @param phi the value of phi with with ep was convicted
     */
    public void convict(NetNode ep, double phi);
}
