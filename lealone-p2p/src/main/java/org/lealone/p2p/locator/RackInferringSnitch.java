/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import org.lealone.net.NetNode;

/**
 * A simple node snitch implementation that assumes datacenter and rack information is encoded
 * in the 2nd and 3rd octets of the ip address, respectively.
 */
public class RackInferringSnitch extends AbstractNetworkTopologySnitch {
    @Override
    public String getRack(NetNode node) {
        return Integer.toString(node.getAddress()[2] & 0xFF, 10);
    }

    @Override
    public String getDatacenter(NetNode node) {
        return Integer.toString(node.getAddress()[1] & 0xFF, 10);
    }
}
