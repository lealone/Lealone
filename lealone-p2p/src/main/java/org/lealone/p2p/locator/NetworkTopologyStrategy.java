/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

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

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.locator.TopologyMetaData.Topology;
import org.lealone.p2p.util.Utils;

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
 * This class also caches the Nodes and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);
    private final INodeSnitch snitch;
    private final Map<String, Integer> datacenters;

    public NetworkTopologyStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions)
            throws ConfigException {
        super(dbName, snitch, configOptions);
        this.snitch = snitch;

        Map<String, Integer> newDatacenters = new HashMap<>();
        if (configOptions != null) {
            for (Entry<String, String> entry : configOptions.entrySet()) {
                String dc = entry.getKey();
                validateOption(dc);
                Integer replicas = Integer.valueOf(entry.getValue());
                newDatacenters.put(dc, replicas);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        if (logger.isDebugEnabled())
            logger.debug("Configured datacenter replicas are {}", Utils.toString(datacenters));
    }

    @Override
    public int getReplicationFactor() {
        int total = 0;
        for (int repFactor : datacenters.values())
            total += repFactor;
        return total;
    }

    @Override
    public void validateOptions() throws ConfigException {
        for (Entry<String, String> e : configOptions.entrySet()) {
            validateOption(e.getKey());
            validateReplicationFactor(e.getValue());
        }
    }

    private void validateOption(String key) throws ConfigException {
        if (key.equalsIgnoreCase("replication_factor"))
            throw new ConfigException(
                    "replication_factor is an option for SimpleStrategy, not NetworkTopologyStrategy");
    }

    @Override
    public Collection<String> recognizedOptions() {
        // We explicitly allow all options
        return null;
    }

    /**
     * calculate nodes in one pass through the tokens by tracking our progress in each DC, rack etc.
     */
    @Override
    public List<NetNode> calculateReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        // we want to preserve insertion order so that the first added node becomes primary
        Set<NetNode> replicas = new LinkedHashSet<>();
        int totalReplicas = 0;
        // replicas we have found in each DC
        Map<String, Set<NetNode>> dcReplicas = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet()) {
            totalReplicas += dc.getValue();
            dcReplicas.put(dc.getKey(), new HashSet<NetNode>(dc.getValue()));
        }

        if (includeOldReplicationNodes)
            totalReplicas -= oldReplicationNodes.size();

        Topology topology = metaData.getTopology();
        // all nodes in each DC, so we can check when we have exhausted all the members of a DC
        Map<String, Set<NetNode>> allNodes = topology.getDatacenterNodes();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, Map<String, Set<NetNode>>> racks = topology.getDatacenterRacks();
        assert !allNodes.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        // tracks the racks we have already placed replicas in
        Map<String, Set<String>> seenRacks = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            seenRacks.put(dc.getKey(), new HashSet<String>());

        // tracks the nodes that we skipped over while looking for unique racks
        // when we relax the rack uniqueness we can append this to the current result
        // so we don't have to wind back the iterator
        Map<String, Set<NetNode>> skippedDcNodes = new HashMap<>(datacenters.size());
        for (Map.Entry<String, Integer> dc : datacenters.entrySet())
            skippedDcNodes.put(dc.getKey(), new LinkedHashSet<NetNode>());

        ArrayList<String> hostIds = metaData.getSortedHostIds();
        Iterator<String> tokenIter = hostIds.iterator();
        while (tokenIter.hasNext() && !hasSufficientReplicas(dcReplicas, allNodes)) {
            String next = tokenIter.next();
            NetNode ep = metaData.getNode(next);
            if (!candidateNodes.contains(ep) || oldReplicationNodes.contains(ep))
                continue;
            String dc = snitch.getDatacenter(ep);
            // have we already found all replicas for this dc?
            if (!datacenters.containsKey(dc) || hasSufficientReplicas(dc, dcReplicas, allNodes))
                continue;
            // can we skip checking the rack?
            if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                dcReplicas.get(dc).add(ep);
                replicas.add(ep);
            } else {
                String rack = snitch.getRack(ep);
                // is this a new rack?
                if (seenRacks.get(dc).contains(rack)) {
                    skippedDcNodes.get(dc).add(ep);
                } else {
                    dcReplicas.get(dc).add(ep);
                    replicas.add(ep);
                    seenRacks.get(dc).add(rack);
                    // if we've run out of distinct racks, add the hosts we skipped past already (up to RF)
                    if (seenRacks.get(dc).size() == racks.get(dc).keySet().size()) {
                        Iterator<NetNode> skippedIt = skippedDcNodes.get(dc).iterator();
                        while (skippedIt.hasNext() && !hasSufficientReplicas(dc, dcReplicas, allNodes)) {
                            NetNode nextSkipped = skippedIt.next();
                            dcReplicas.get(dc).add(nextSkipped);
                            replicas.add(nextSkipped);
                        }
                    }
                }
            }
        }

        // 不够时，从原来的复制节点中取
        if (!oldReplicationNodes.isEmpty() && replicas.size() < totalReplicas) {
            List<NetNode> oldNodes = calculateReplicationNodes(metaData, new HashSet<>(0), oldReplicationNodes,
                    includeOldReplicationNodes);

            Iterator<NetNode> old = oldNodes.iterator();
            while (replicas.size() < totalReplicas && old.hasNext()) {
                NetNode ep = old.next();
                if (!replicas.contains(ep))
                    replicas.add(ep);
            }
        }

        return new ArrayList<NetNode>(replicas);
    }

    private boolean hasSufficientReplicas(String dc, Map<String, Set<NetNode>> dcReplicas,
            Map<String, Set<NetNode>> allNodes) {
        return dcReplicas.get(dc).size() >= Math.min(allNodes.get(dc).size(), getReplicationFactor(dc));
    }

    private boolean hasSufficientReplicas(Map<String, Set<NetNode>> dcReplicas, Map<String, Set<NetNode>> allNodes) {
        for (String dc : datacenters.keySet())
            if (!hasSufficientReplicas(dc, dcReplicas, allNodes))
                return false;
        return true;
    }

    private int getReplicationFactor(String dc) {
        Integer replicas = datacenters.get(dc);
        return replicas == null ? 0 : replicas;
    }
}
