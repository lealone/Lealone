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
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Pair;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gossip.FailureDetector;
import org.lealone.p2p.server.P2pServer;

public class TopologyMetaData {

    private static final Logger logger = LoggerFactory.getLogger(TopologyMetaData.class);

    /** Maintains node to host ID map of every node in the cluster */
    private final Map<NetNode, String> nodeToHostIdMap;
    private final Map<String, NetNode> hostIdToNodeMap;
    private final Topology topology;

    // don't need to record host ID here since it's still part of nodeToHostIdMap until it's done leaving
    private final Set<NetNode> leavingNodes = new HashSet<>();
    private final AtomicReference<TopologyMetaData> cachedMap = new AtomicReference<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public TopologyMetaData() {
        this(new HashMap<>(), new HashMap<>(), new Topology());
    }

    private TopologyMetaData(Map<NetNode, String> nodeToHostIdMap, Map<String, NetNode> hostIdToNodeMap,
            Topology topology) {
        this.nodeToHostIdMap = nodeToHostIdMap;
        this.hostIdToNodeMap = hostIdToNodeMap;
        this.topology = topology;
    }

    /**
     * Store an end-point to host ID mapping.
     * Each ID must be unique, and cannot be changed after the fact.
     *
     * @param hostId
     * @param node
     */
    public void updateHostId(String hostId, NetNode node) {
        assert hostId != null;
        assert node != null;

        lock.writeLock().lock();
        try {
            NetNode storedEp = hostIdToNodeMap.get(hostId);
            if (storedEp != null) {
                if (!storedEp.equals(node) && (FailureDetector.instance.isAlive(storedEp))) {
                    throw new RuntimeException(String.format("Host ID collision between active node %s and %s (id=%s)",
                            storedEp, node, hostId));
                }
            } else {
                topology.addNode(node);
            }

            String storedId = nodeToHostIdMap.get(node);
            if ((storedId != null) && (!storedId.equals(hostId)))
                logger.warn("Changing {}'s host ID from {} to {}", node, storedId, hostId);

            nodeToHostIdMap.put(node, hostId);
            hostIdToNodeMap.put(hostId, node);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Return the unique host ID for an end-point. */
    public String getHostId(NetNode node) {
        lock.readLock().lock();
        try {
            return nodeToHostIdMap.get(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public NetNode getNode(String hostId) {
        lock.readLock().lock();
        try {
            return hostIdToNodeMap.get(hostId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the node-to-id map for read-only operations */
    public Map<NetNode, String> getNodeToHostIdMapForReading() {
        lock.readLock().lock();
        try {
            Map<NetNode, String> readMap = new HashMap<>();
            readMap.putAll(nodeToHostIdMap);
            return readMap;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addLeavingNode(NetNode node) {
        assert node != null;

        lock.writeLock().lock();
        try {
            leavingNodes.add(node);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeNode(NetNode node) {
        assert node != null;

        lock.writeLock().lock();
        try {
            topology.removeNode(node);
            leavingNodes.remove(node);
            hostIdToNodeMap.remove(nodeToHostIdMap.get(node));
            nodeToHostIdMap.remove(node);
            invalidateCachedRings();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isMember(NetNode node) {
        assert node != null;

        lock.readLock().lock();
        try {
            return nodeToHostIdMap.containsKey(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(NetNode node) {
        assert node != null;

        lock.readLock().lock();
        try {
            return leavingNodes.contains(node);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TopologyMetaData with only nodeToHostIdMap.
     * That is, leaving nodes are not included in the copy.
     */
    private TopologyMetaData cloneOnlyHostIdMap() {
        lock.readLock().lock();
        try {
            return new TopologyMetaData(new HashMap<>(nodeToHostIdMap), new HashMap<>(hostIdToNodeMap),
                    new Topology(topology));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TopologyMetaData with only nodeToHostIdMap, i.e., the same as cloneOnlyHostIdMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public TopologyMetaData getCacheOnlyHostIdMap() {
        TopologyMetaData tm = cachedMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd
        synchronized (this) {
            if ((tm = cachedMap.get()) != null)
                return tm;

            tm = cloneOnlyHostIdMap();
            cachedMap.set(tm);
            return tm;
        }
    }

    public ArrayList<String> getSortedHostIds() {
        return new ArrayList<>(hostIdToNodeMap.keySet());
    }

    public Set<NetNode> getAllNodes() {
        lock.readLock().lock();
        try {
            return new HashSet<>(nodeToHostIdMap.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify leavingNodes */
    public Set<NetNode> getLeavingNodes() {
        lock.readLock().lock();
        try {
            return new HashSet<>(leavingNodes);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** used by tests */
    public void clearUnsafe() {
        lock.writeLock().lock();
        try {
            nodeToHostIdMap.clear();
            hostIdToNodeMap.clear();
            leavingNodes.clear();
            topology.clear();
            invalidateCachedRings();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return the Topology map of nodes to DCs + Racks
     *
     * This is only allowed when a copy has been made of TopologyMetaData, to avoid concurrent modifications
     * when Topology methods are subsequently used by the caller.
     */
    public Topology getTopology() {
        assert this != P2pServer.instance.getTopologyMetaData();
        return topology;
    }

    public long getRingVersion() {
        return ringVersion;
    }

    public void invalidateCachedRings() {
        ringVersion++;
        cachedMap.set(null);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try {
            String lineSeparator = System.getProperty("line.separator");
            sb.append("HostIds: " + getSortedHostIds());
            if (!leavingNodes.isEmpty()) {
                sb.append("Leaving Nodes:");
                sb.append(lineSeparator);
                for (NetNode ep : leavingNodes) {
                    sb.append(ep);
                    sb.append(lineSeparator);
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return sb.toString();
    }

    /**
     * Tracks the assignment of racks and nodes in each datacenter for all the "normal" nodes
     * in this TopologyMetaData. This allows faster calculation of nodes in NetworkTopologyStrategy.
     */
    public static class Topology {
        /** multi-map of DC to nodes in that DC */
        private final Map<String, Set<NetNode>> dcNodes;
        /** map of DC to multi-map of rack to nodes in that rack */
        private final Map<String, Map<String, Set<NetNode>>> dcRacks;
        /** reverse-lookup map for node to current known dc/rack assignment */
        private final Map<NetNode, Pair<String, String>> currentLocations;

        protected Topology() {
            dcNodes = new HashMap<>();
            dcRacks = new HashMap<>();
            currentLocations = new HashMap<>();
        }

        protected void clear() {
            dcNodes.clear();
            dcRacks.clear();
            currentLocations.clear();
        }

        /**
         * construct deep-copy of other
         */
        protected Topology(Topology other) {
            dcNodes = new HashMap<>(other.dcNodes.size());
            for (String dc : other.dcNodes.keySet()) {
                dcNodes.put(dc, new HashSet<>(other.dcNodes.get(dc)));
            }
            dcRacks = new HashMap<>(other.dcRacks.size());
            for (String dc : other.dcRacks.keySet()) {
                Map<String, Set<NetNode>> oldRacks = other.dcRacks.get(dc);
                Map<String, Set<NetNode>> newRacks = new HashMap<>(oldRacks.size());
                dcRacks.put(dc, newRacks);
                for (String rack : oldRacks.keySet()) {
                    newRacks.put(rack, new HashSet<>(oldRacks.get(rack)));
                }
            }
            currentLocations = new HashMap<>(other.currentLocations);
        }

        /**
         * Stores current DC/rack assignment for ep
         */
        protected void addNode(NetNode ep) {
            INodeSnitch snitch = ConfigDescriptor.getNodeSnitch();
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            Pair<String, String> current = currentLocations.get(ep);
            if (current != null) {
                if (current.left.equals(dc) && current.right.equals(rack))
                    return;
                dcRacks.get(current.left).get(current.right).remove(ep);
                dcNodes.get(current.left).remove(ep);
            }
            Set<NetNode> nodes = dcNodes.get(dc);
            if (nodes == null) {
                nodes = new HashSet<>();
                dcNodes.put(dc, nodes);
            }
            nodes.add(ep);

            if (!dcRacks.containsKey(dc))
                dcRacks.put(dc, new HashMap<>());

            if (!dcRacks.get(dc).containsKey(rack))
                dcRacks.get(dc).put(rack, new HashSet<>());
            dcRacks.get(dc).get(rack).add(ep);

            currentLocations.put(ep, Pair.create(dc, rack));
        }

        /**
         * Removes current DC/rack assignment for ep
         */
        protected void removeNode(NetNode ep) {
            if (!currentLocations.containsKey(ep))
                return;
            Pair<String, String> current = currentLocations.remove(ep);
            dcNodes.get(current.left).remove(ep);
            dcRacks.get(current.left).get(current.right).remove(ep);
        }

        /**
         * @return multi-map of DC to nodes in that DC
         */
        public Map<String, Set<NetNode>> getDatacenterNodes() {
            return dcNodes;
        }

        /**
         * @return map of DC to multi-map of rack to nodes in that rack
         */
        public Map<String, Map<String, Set<NetNode>>> getDatacenterRacks() {
            return dcRacks;
        }
    }
}
