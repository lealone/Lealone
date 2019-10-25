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
import org.lealone.net.NetEndpoint;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.FailureDetector;
import org.lealone.p2p.server.P2pServer;

public class TopologyMetaData {

    private static final Logger logger = LoggerFactory.getLogger(TopologyMetaData.class);

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final Map<NetEndpoint, String> endpointToHostIdMap;
    private final Map<String, NetEndpoint> hostIdToEndpointMap;
    private final Topology topology;

    // don't need to record host ID here since it's still part of endpointToHostIdMap until it's done leaving
    private final Set<NetEndpoint> leavingEndpoints = new HashSet<>();
    private final AtomicReference<TopologyMetaData> cachedMap = new AtomicReference<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public TopologyMetaData() {
        this(new HashMap<>(), new HashMap<>(), new Topology());
    }

    private TopologyMetaData(Map<NetEndpoint, String> endpointToHostIdMap, Map<String, NetEndpoint> hostIdToEndpointMap,
            Topology topology) {
        this.endpointToHostIdMap = endpointToHostIdMap;
        this.hostIdToEndpointMap = hostIdToEndpointMap;
        this.topology = topology;
    }

    /**
     * Store an end-point to host ID mapping.
     * Each ID must be unique, and cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    public void updateHostId(String hostId, NetEndpoint endpoint) {
        assert hostId != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try {
            NetEndpoint storedEp = hostIdToEndpointMap.get(hostId);
            if (storedEp != null) {
                if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp))) {
                    throw new RuntimeException(String.format(
                            "Host ID collision between active endpoint %s and %s (id=%s)", storedEp, endpoint, hostId));
                }
            } else {
                topology.addEndpoint(endpoint);
            }

            String storedId = endpointToHostIdMap.get(endpoint);
            if ((storedId != null) && (!storedId.equals(hostId)))
                logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, hostId);

            endpointToHostIdMap.put(endpoint, hostId);
            hostIdToEndpointMap.put(hostId, endpoint);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Return the unique host ID for an end-point. */
    public String getHostId(NetEndpoint endpoint) {
        lock.readLock().lock();
        try {
            return endpointToHostIdMap.get(endpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** Return the end-point for a unique host ID */
    public NetEndpoint getEndpoint(String hostId) {
        lock.readLock().lock();
        try {
            return hostIdToEndpointMap.get(hostId);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** @return a copy of the endpoint-to-id map for read-only operations */
    public Map<NetEndpoint, String> getEndpointToHostIdMapForReading() {
        lock.readLock().lock();
        try {
            Map<NetEndpoint, String> readMap = new HashMap<>();
            readMap.putAll(endpointToHostIdMap);
            return readMap;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addLeavingEndpoint(NetEndpoint endpoint) {
        assert endpoint != null;

        lock.writeLock().lock();
        try {
            leavingEndpoints.add(endpoint);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeEndpoint(NetEndpoint endpoint) {
        assert endpoint != null;

        lock.writeLock().lock();
        try {
            topology.removeEndpoint(endpoint);
            leavingEndpoints.remove(endpoint);
            hostIdToEndpointMap.remove(endpointToHostIdMap.get(endpoint));
            endpointToHostIdMap.remove(endpoint);
            invalidateCachedRings();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isMember(NetEndpoint endpoint) {
        assert endpoint != null;

        lock.readLock().lock();
        try {
            return endpointToHostIdMap.containsKey(endpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isLeaving(NetEndpoint endpoint) {
        assert endpoint != null;

        lock.readLock().lock();
        try {
            return leavingEndpoints.contains(endpoint);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Create a copy of TopologyMetaData with only endpointToHostIdMap.
     * That is, leaving endpoints are not included in the copy.
     */
    private TopologyMetaData cloneOnlyHostIdMap() {
        lock.readLock().lock();
        try {
            return new TopologyMetaData(new HashMap<>(endpointToHostIdMap), new HashMap<>(hostIdToEndpointMap),
                    new Topology(topology));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TopologyMetaData with only endpointToHostIdMap, i.e., the same as cloneOnlyHostIdMap but
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
        return new ArrayList<>(hostIdToEndpointMap.keySet());
    }

    public Set<NetEndpoint> getAllEndpoints() {
        lock.readLock().lock();
        try {
            return new HashSet<>(endpointToHostIdMap.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify leavingEndpoints */
    public Set<NetEndpoint> getLeavingEndpoints() {
        lock.readLock().lock();
        try {
            return new HashSet<>(leavingEndpoints);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** used by tests */
    public void clearUnsafe() {
        lock.writeLock().lock();
        try {
            endpointToHostIdMap.clear();
            hostIdToEndpointMap.clear();
            leavingEndpoints.clear();
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
            if (!leavingEndpoints.isEmpty()) {
                sb.append("Leaving Endpoints:");
                sb.append(lineSeparator);
                for (NetEndpoint ep : leavingEndpoints) {
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
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TopologyMetaData. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology {
        /** multi-map of DC to endpoints in that DC */
        private final Map<String, Set<NetEndpoint>> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final Map<String, Map<String, Set<NetEndpoint>>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final Map<NetEndpoint, Pair<String, String>> currentLocations;

        protected Topology() {
            dcEndpoints = new HashMap<>();
            dcRacks = new HashMap<>();
            currentLocations = new HashMap<>();
        }

        protected void clear() {
            dcEndpoints.clear();
            dcRacks.clear();
            currentLocations.clear();
        }

        /**
         * construct deep-copy of other
         */
        protected Topology(Topology other) {
            dcEndpoints = new HashMap<>(other.dcEndpoints.size());
            for (String dc : other.dcEndpoints.keySet()) {
                dcEndpoints.put(dc, new HashSet<>(other.dcEndpoints.get(dc)));
            }
            dcRacks = new HashMap<>(other.dcRacks.size());
            for (String dc : other.dcRacks.keySet()) {
                Map<String, Set<NetEndpoint>> oldRacks = other.dcRacks.get(dc);
                Map<String, Set<NetEndpoint>> newRacks = new HashMap<>(oldRacks.size());
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
        protected void addEndpoint(NetEndpoint ep) {
            IEndpointSnitch snitch = ConfigDescriptor.getEndpointSnitch();
            String dc = snitch.getDatacenter(ep);
            String rack = snitch.getRack(ep);
            Pair<String, String> current = currentLocations.get(ep);
            if (current != null) {
                if (current.left.equals(dc) && current.right.equals(rack))
                    return;
                dcRacks.get(current.left).get(current.right).remove(ep);
                dcEndpoints.get(current.left).remove(ep);
            }
            Set<NetEndpoint> endpoints = dcEndpoints.get(dc);
            if (endpoints == null) {
                endpoints = new HashSet<>();
                dcEndpoints.put(dc, endpoints);
            }
            endpoints.add(ep);

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
        protected void removeEndpoint(NetEndpoint ep) {
            if (!currentLocations.containsKey(ep))
                return;
            Pair<String, String> current = currentLocations.remove(ep);
            dcEndpoints.get(current.left).remove(ep);
            dcRacks.get(current.left).get(current.right).remove(ep);
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Map<String, Set<NetEndpoint>> getDatacenterEndpoints() {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public Map<String, Map<String, Set<NetEndpoint>>> getDatacenterRacks() {
            return dcRacks;
        }
    }
}
