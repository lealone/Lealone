/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetNode;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class SimpleStrategy extends AbstractReplicationStrategy {

    public SimpleStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
        super(dbName, snitch, configOptions);
    }

    @Override
    public int getReplicationFactor() {
        return Integer.parseInt(configOptions.get("replication_factor"));
    }

    @Override
    public void validateOptions() throws ConfigException {
        String rf = configOptions.get("replication_factor");
        if (rf == null)
            throw new ConfigException("SimpleStrategy requires a replication_factor strategy option.");
        validateReplicationFactor(rf);
    }

    @Override
    public Collection<String> recognizedOptions() {
        return Collections.<String> singleton("replication_factor");
    }

    @Override
    public List<NetNode> calculateReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        int replicas = getReplicationFactor();
        if (includeOldReplicationNodes)
            replicas -= oldReplicationNodes.size();
        ArrayList<String> hostIds = metaData.getSortedHostIds();
        List<NetNode> nodes = new ArrayList<NetNode>(replicas);

        if (hostIds.isEmpty())
            return nodes;

        Iterator<String> iter = hostIds.iterator();
        while (nodes.size() < replicas && iter.hasNext()) {
            NetNode ep = metaData.getNode(iter.next());
            if (candidateNodes.contains(ep) && !oldReplicationNodes.contains(ep) && !nodes.contains(ep))
                nodes.add(ep);
        }

        // 不够时，从原来的复制节点中取
        if (nodes.size() < replicas) {
            Iterator<NetNode> old = oldReplicationNodes.iterator();
            while (nodes.size() < replicas && old.hasNext()) {
                NetNode ep = old.next();
                if (!nodes.contains(ep))
                    nodes.add(ep);
            }
        }
        return nodes;
    }
}
