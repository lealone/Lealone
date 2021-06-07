/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.net.UnknownHostException;

/**
 * MBean exposing standard Snitch info
 */
public interface NodeSnitchInfoMBean {
    /**
     * Provides the Rack name depending on the respective snitch used, given the host name/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getRack(String host) throws UnknownHostException;

    /**
     * Provides the Datacenter name depending on the respective snitch used, given the hostname/ip
     * @param host
     * @throws UnknownHostException
     */
    public String getDatacenter(String host) throws UnknownHostException;

    /**
     * Provides the snitch name of the cluster
     * @return Snitch name
     */
    public String getSnitchName();
}
