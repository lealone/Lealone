/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.gossip;

import java.net.UnknownHostException;
import java.util.Map;

public interface FailureDetectorMBean {
    public void dumpInterArrivalTimes();

    public void setPhiConvictThreshold(double phi);

    public double getPhiConvictThreshold();

    public String getAllNodeStates();

    public String getNodeState(String address) throws UnknownHostException;

    public Map<String, String> getSimpleStates();

    public int getDownNodeCount();

    public int getUpNodeCount();
}
