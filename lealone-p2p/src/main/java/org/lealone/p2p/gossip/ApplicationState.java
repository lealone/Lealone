/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

public enum ApplicationState {
    HOST_ID,
    TCP_NODE,
    P2P_NODE,
    DC,
    RACK,
    RELEASE_VERSION,
    NET_VERSION,
    STATUS,
    LOAD,
    SEVERITY,
    INTERNAL_IP,
    REMOVAL_COORDINATOR,
    // pad to allow adding new states to existing cluster
    X1,
    X2,
    X3,
    X4,
    X5,
    X6,
    X7,
    X8,
    X9,
    X10,
}
