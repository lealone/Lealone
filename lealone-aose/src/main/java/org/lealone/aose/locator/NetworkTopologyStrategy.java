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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.lealone.aose.locator.TopologyMetaData.Topology;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetEndpoint;

import com.google.common.collect.Multimap;

/**
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * <p/>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * <p/>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);
    private final IEndpointSnitch snitch;
    private final Map<String, Integer> datacenters;

    public NetworkTopologyStrategy(String dbName, TopologyMetaData metaData, IEndpointSnitch snitch,
            Map<String, String> configOptions) throws ConfigException {
        super(dbName, metaData, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> newDatacenters = new HashMap<>();
        if (configOptions != null) {
            for (Entry<String, String> entry : configOptions.entrySet()) {
                String dc = entry.getKey();
                if (dc.equalsIgnoreCase("replication_factor"))
                    throw new ConfigException(
                            "replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
                Integer replicas = Integer.valueOf(entry.getValue());
                newDatacenters.put(dc, replicas);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        if (logger.isDebugEnabled())
            logger.debug("Configured datacenter replicas are {}", Utils.toString(datacenters));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC, rack etc.
     */
    @Override
    @SuppressWarnings("serial")
    public List<NetEndpoint> calculateReplicationEndpoints(String searchHostId, TopologyMetaData metaData,
            Set<NetEndpoint> candidateEndpoints) {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        Set<NetEndpoint> replicas = new LinkedHashSet<>();
        // replicas we have found in each DC
        Map<String, Set<NetEndpoint>> dcReplicas = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            dcReplicas.put(dc.getKey(), new HashSet<NetEndpoint>(dc.getValue()));

        Topology topology = metaData.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, NetEndpoint> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, Multimap<String, NetEndpoint>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        // tracks the racks we have already placed replicas in
        Map<String, Set<String>> seenRacks = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            seenRacks.put(dc.getKey(), new HashSet<String>());

        // tracks the endpoints that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result
        // so we don't have to wind back the iterator
        Map<String, Set<NetEndpoint>> skippedDcEndpoints = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            skippedDcEndpoints.put(dc.getKey(), new LinkedHashSet<NetEndpoint>());

        ArrayList<String> hostIds = metaData.sortedHostIds();
        Iterator<String> tokenIter = hostIds.iterator();
        while (tokenIter.hasNext() && !hasSufficientReplicas(dcReplicas, allEndpoints)) {
            String next = tokenIter.next();
            NetEndpoint ep = metaData.getEndpointForHostId(next);
            if (!candidateEndpoints.contains(ep))
                continue;
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc?
            if (!datacenters.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allEndpoints))
                continue;
            // can we skip checking the rack?
            if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                dcReplicas.get(dc).add(ep);
                replicas.add(ep);
            } else {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (seenRacks.get(dc).contains(rack)) {
                    skippedDcEndpoints.get(dc).add(ep);
                } else {
                    dcReplicas.get(dc).add(ep);
                    replicas.add(ep);
                    seenRacks.get(dc).add(rack);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                        Iterator<NetEndpoint> skippedIt = skippedDcEndpoints.get(dc).iterator();
                        while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allEndpoints)) {
                            NetEndpoint nextSkipped = skippedIt.next();
                            dcReplicas.get(dc).add(nextSkipped);
                            replicas.add(nextSkipped);
                        }
                    }
                }
            }
        }

        return new ArrayList<NetEndpoint>(replicas);
    }

    private boolean hasSufficientReplicas(String dc, Map<String, Set<NetEndpoint>> dcReplicas,
            Multimap<String, NetEndpoint> allEndpoints) {
        return dcReplicas.get(dc).size() >= Math.min(allEndpoints.get(dc).size(), getReplicationFactor(dc));
    }

    private boolean hasSufficientReplicas(Map<String, Set<NetEndpoint>> dcReplicas,
            Multimap<String, NetEndpoint> allEndpoints) {
        for (String dc : datacenters.keySet())
            if (!hasSufficientReplicas(dc, dcReplicas, allEndpoints))
                return false;
        return true;
    }

    @Override
    public int getReplicationFactor() {
        int total = 0;
        for (int repFactor : datacenters.values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc) {
        Integer replicas = datacenters.get(dc);
        return replicas == null ? 0 : replicas;
    }

    public Set<String> getDatacenters() {
        return datacenters.keySet();
    }

    @Override
    public void validateOptions() throws ConfigException {
        for (Entry<String, String> e : this.configOptions.entrySet()) {
            if (e.getKey().equalsIgnoreCase("replication_factor"))
                throw new ConfigException(
                        "replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
            validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public Collection<String> recognizedOptions() {
        // We explicitely allow all options
        return null;
    }
}
