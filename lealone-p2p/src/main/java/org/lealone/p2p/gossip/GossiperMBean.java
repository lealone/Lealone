/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import java.net.UnknownHostException;

public interface GossiperMBean {

    public void assassinateNode(String address) throws UnknownHostException;

    public long getNodeDowntime(String address) throws UnknownHostException;

    public int getCurrentGenerationNumber(String address) throws UnknownHostException;

}