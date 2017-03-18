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
package org.lealone.aose.locator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.FailureDetector;
import org.lealone.aose.server.P2pServer;
import org.lealone.aose.util.Pair;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetEndpoint;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

public class TopologyMetaData {
    private static final Logger logger = LoggerFactory.getLogger(TopologyMetaData.class);

    /** Maintains endpoint to host ID map of every node in the cluster */
    private final BiMap<NetEndpoint, String> endpointToHostIdMap;

    // (don't need to record Token here since it's still part of tokenToEndpointMap until it's done leaving)
    private final Set<NetEndpoint> leavingEndpoints = new HashSet<>();

    /* Use this lock for manipulating the token map */
    private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final Topology topology;

    private final AtomicReference<TopologyMetaData> cachedMap = new AtomicReference<>();

    // signals replication strategies that nodes have joined or left the ring and they need to recompute ownership
    private volatile long ringVersion = 0;

    public TopologyMetaData() {
        this(HashBiMap.<NetEndpoint, String> create(), new Topology());
    }

    private TopologyMetaData(BiMap<NetEndpoint, String> endpointsMap, Topology topology) {
        this.topology = topology;
        endpointToHostIdMap = endpointsMap;
    }

    /**
     * Store an end-point to host ID mapping.  Each ID must be unique, and
     * cannot be changed after the fact.
     *
     * @param hostId
     * @param endpoint
     */
    public void updateHostId(String hostId, NetEndpoint endpoint) {
        assert hostId != null;
        assert endpoint != null;

        lock.writeLock().lock();
        try {
            NetEndpoint storedEp = endpointToHostIdMap.inverse().get(hostId);
            if (storedEp != null) {
                if (!storedEp.equals(endpoint) && (FailureDetector.instance.isAlive(storedEp))) {
                    throw new RuntimeException(String.format(
                            "Host ID collision between active endpoint %s and %s (id=%s)", storedEp, endpoint, hostId));
                }
            }

            String storedId = endpointToHostIdMap.get(endpoint);
            if ((storedId != null) && (!storedId.equals(hostId)))
                logger.warn("Changing {}'s host ID from {} to {}", endpoint, storedId, hostId);

            endpointToHostIdMap.forcePut(endpoint, hostId);
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
    public NetEndpoint getEndpointForHostId(String hostId) {
        lock.readLock().lock();
        try {
            return endpointToHostIdMap.inverse().get(hostId);
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
     * Create a copy of TopologyMetaData with only tokenToEndpointMap. That is, pending ranges,
     * bootstrap tokens and leaving endpoints are not included in the copy.
     */
    public TopologyMetaData cloneOnlyTokenMap() {
        lock.readLock().lock();
        try {
            return new TopologyMetaData(HashBiMap.create(endpointToHostIdMap), new Topology(topology));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Return a cached TopologyMetaData with only tokenToEndpointMap, i.e., the same as cloneOnlyTokenMap but
     * uses a cached copy that is invalided when the ring changes, so in the common case
     * no extra locking is required.
     *
     * Callers must *NOT* mutate the returned metadata object.
     */
    public TopologyMetaData cachedOnlyTokenMap() {
        TopologyMetaData tm = cachedMap.get();
        if (tm != null)
            return tm;

        // synchronize to prevent thundering herd (lealone-6345)
        synchronized (this) {
            if ((tm = cachedMap.get()) != null)
                return tm;

            tm = cloneOnlyTokenMap();
            cachedMap.set(tm);
            return tm;
        }
    }

    public ArrayList<String> sortedHostIds() {
        return new ArrayList<>(endpointToHostIdMap.inverse().keySet());
    }

    public Set<NetEndpoint> getAllEndpoints() {
        lock.readLock().lock();
        try {
            return ImmutableSet.copyOf(endpointToHostIdMap.keySet());
        } finally {
            lock.readLock().unlock();
        }
    }

    /** caller should not modify leavingEndpoints */
    public Set<NetEndpoint> getLeavingEndpoints() {
        lock.readLock().lock();
        try {
            return ImmutableSet.copyOf(leavingEndpoints);
        } finally {
            lock.readLock().unlock();
        }
    }

    /** used by tests */
    public void clearUnsafe() {
        lock.writeLock().lock();
        try {
            endpointToHostIdMap.clear();
            leavingEndpoints.clear();
            topology.clear();
            invalidateCachedRings();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        lock.readLock().lock();
        try {
            String lineSeparator = System.getProperty("line.separator");
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

    public String getNextHostId(String hostId) {
        ArrayList<String> list = sortedHostIds();
        int index = list.indexOf(hostId);
        if (index == -1 || index == list.size() - 1)
            index = 0;
        else
            index++;

        return list.get(index);
    }

    /**
     * Tracks the assignment of racks and endpoints in each datacenter for all the "normal" endpoints
     * in this TopologyMetaData. This allows faster calculation of endpoints in NetworkTopologyStrategy.
     */
    public static class Topology {
        /** multi-map of DC to endpoints in that DC */
        private final Multimap<String, NetEndpoint> dcEndpoints;
        /** map of DC to multi-map of rack to endpoints in that rack */
        private final Map<String, Multimap<String, NetEndpoint>> dcRacks;
        /** reverse-lookup map for endpoint to current known dc/rack assignment */
        private final Map<NetEndpoint, Pair<String, String>> currentLocations;

        protected Topology() {
            dcEndpoints = HashMultimap.create();
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
            dcEndpoints = HashMultimap.create(other.dcEndpoints);
            dcRacks = new HashMap<>();
            for (String dc : other.dcRacks.keySet())
                dcRacks.put(dc, HashMultimap.create(other.dcRacks.get(dc)));
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
                dcRacks.get(current.left).remove(current.right, ep);
                dcEndpoints.remove(current.left, ep);
            }

            dcEndpoints.put(dc, ep);

            if (!dcRacks.containsKey(dc))
                dcRacks.put(dc, HashMultimap.<String, NetEndpoint> create());
            dcRacks.get(dc).put(rack, ep);

            currentLocations.put(ep, Pair.create(dc, rack));
        }

        /**
         * Removes current DC/rack assignment for ep
         */
        protected void removeEndpoint(NetEndpoint ep) {
            if (!currentLocations.containsKey(ep))
                return;
            Pair<String, String> current = currentLocations.remove(ep);
            dcEndpoints.remove(current.left, ep);
            dcRacks.get(current.left).remove(current.right, ep);
        }

        /**
         * @return multi-map of DC to endpoints in that DC
         */
        public Multimap<String, NetEndpoint> getDatacenterEndpoints() {
            return dcEndpoints;
        }

        /**
         * @return map of DC to multi-map of rack to endpoints in that rack
         */
        public Map<String, Multimap<String, NetEndpoint>> getDatacenterRacks() {
            return dcRacks;
        }
    }
}
