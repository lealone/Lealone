/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.cluster.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import javax.management.NotificationEmitter;

public interface StorageServiceMBean extends NotificationEmitter {
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLiveNodes();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined
     * by this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getUnreachableNodes();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getJoiningNodes();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLeavingNodes();

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getMovingNodes();

    /**
     * Fetch string representations of the tokens for this node.
     *
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens();

    /**
     * Fetch string representations of the tokens for a specified node.
     *
     * @param endpoint string representation of an node
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens(String endpoint) throws UnknownHostException;

    /**
     * Fetch a string representation of the lealone version.
     * @return A string representation of the lealone version.
     */
    public String getReleaseVersion();

    /**
     * Retrieve a map of range to end points that describe the ring topology
     * of a lealone cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a lealone cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace);

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring topology
     * @param keyspace the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping
     * ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    public Map<String, String> getTokenToEndpointMap();

    /** Retrieve this hosts unique ID */
    public String getLocalHostId();

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getHostIdMap();

    /** Human-readable load value */
    public String getLoadString();

    /** Human-readable load value.  Keys are IP addresses. */
    public Map<String, String> getLoadMap();

    /**
     * Return the generation value for this node.
     *
     * @return generation number
     */
    public int getCurrentGenerationNumber();

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName keyspace name
     * @param cf Column family name
     * @param key - key for which we need to find the endpoint return value -
     * the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key);

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key);

    /**
     * removeToken removes token (and all data associated with
     * enpoint that had it) from the ring
     */
    public void removeNode(String token);

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus();

    /**
     * Force a remove operation to finish.
     */
    public void forceRemoveCompletion();

    /** get the operational mode (leaving, joining, normal, decommissioned, client) **/
    public String getOperationMode();

    /** Returns whether the storage service is starting or not */
    public boolean isStarting();

    /**
     * given a list of tokens (representing the nodes in the cluster), returns
     *   a mapping from "token -> %age of cluster owned by that token"
     */
    public Map<InetAddress, Float> getOwnership();

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping();

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping();

    // allows a user to see whether gossip is running or not
    public boolean isGossipRunning();

    // to determine if gossip is disabled
    public boolean isInitialized();

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException;

    public boolean isJoined();

    public void deliverHints(String host) throws UnknownHostException;

    /** Returns the name of the cluster */
    public String getClusterName();

    /** Returns the cluster partitioner */
    public String getPartitionerName();

    /** Sets the hinted handoff throttle in kb per second, per delivery thread. */
    public void setHintedHandoffThrottleInKB(int throttleInKB);
}
