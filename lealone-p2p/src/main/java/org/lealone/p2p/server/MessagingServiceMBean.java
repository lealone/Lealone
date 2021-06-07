/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import java.net.UnknownHostException;
import java.util.Map;

/**
 * MBean exposing MessagingService metrics.
 * - OutboundConnectionPools - Command/Response - Pending/Completed Tasks
 */
public interface MessagingServiceMBean {

    public int getVersion(String address) throws UnknownHostException;

    /**
     * Pending tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    public Map<String, Integer> getResponsePendingTasks();

    /**
     * Completed tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    public Map<String, Long> getResponseCompletedTasks();

    /**
     * dropped message counts for server lifetime
     */
    public Map<String, Integer> getDroppedMessages();

    /**
     * Total number of timeouts happened on this node
     */
    public long getTotalTimeouts();

    /**
     * Number of timeouts per host
     */
    public Map<String, Long> getTimeoutsPerHost();
}
