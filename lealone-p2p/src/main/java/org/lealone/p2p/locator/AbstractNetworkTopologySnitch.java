/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import org.lealone.net.NetNode;

/**
 * An node snitch tells lealone information about network topology that it can use to route
 * requests more efficiently.
 */
public abstract class AbstractNetworkTopologySnitch extends AbstractNodeSnitch {
    /**
     * Return the rack for which an node resides in
     * @param node a specified node
     * @return string of rack
     */
    @Override
    abstract public String getRack(NetNode node);

    /**
     * Return the data center for which an node resides in
     * @param node a specified node
     * @return string of data center
     */
    @Override
    abstract public String getDatacenter(NetNode node);

    @Override
    public int compareNodes(NetNode address, NetNode a1, NetNode a2) {
        if (address.equals(a1) && !address.equals(a2))
            return -1;
        if (address.equals(a2) && !address.equals(a1))
            return 1;

        String addressDatacenter = getDatacenter(address);
        String a1Datacenter = getDatacenter(a1);
        String a2Datacenter = getDatacenter(a2);
        if (addressDatacenter.equals(a1Datacenter) && !addressDatacenter.equals(a2Datacenter))
            return -1;
        if (addressDatacenter.equals(a2Datacenter) && !addressDatacenter.equals(a1Datacenter))
            return 1;

        String addressRack = getRack(address);
        String a1Rack = getRack(a1);
        String a2Rack = getRack(a2);
        if (addressRack.equals(a1Rack) && !addressRack.equals(a2Rack))
            return -1;
        if (addressRack.equals(a2Rack) && !addressRack.equals(a1Rack))
            return 1;
        return 0;
    }
}
