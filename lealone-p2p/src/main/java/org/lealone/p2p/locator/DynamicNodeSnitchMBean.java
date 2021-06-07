/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import org.lealone.net.NetNode;

public interface DynamicNodeSnitchMBean {
    public Map<NetNode, Double> getScores();

    public int getUpdateInterval();

    public int getResetInterval();

    public double getBadnessThreshold();

    public String getSubsnitchClassName();

    public List<Double> dumpTimings(String hostname) throws UnknownHostException;

    /**
     * Use this if you want to specify a severity; it can be negative
     * Example: Page cache is cold and you want data to be sent 
     *          though it is not preferred one.
     */
    public void setSeverity(double severity);

    public double getSeverity();
}
