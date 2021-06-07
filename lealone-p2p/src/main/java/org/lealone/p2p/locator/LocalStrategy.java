/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;

public class LocalStrategy extends AbstractReplicationStrategy {

    public LocalStrategy(String dbName, INodeSnitch snitch, Map<String, String> configOptions) {
        super(dbName, snitch, configOptions);
    }

    @Override
    public int getReplicationFactor() {
        return 1;
    }

    @Override
    public void validateOptions() throws ConfigException {
    }

    @Override
    public Collection<String> recognizedOptions() {
        // LocalStrategy doesn't expect any options.
        return Collections.<String> emptySet();
    }

    /**
     * 覆盖默认实现，默认实现里要拷贝TopologyMetaData
     */
    @Override
    public List<NetNode> getReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        ArrayList<NetNode> list = new ArrayList<NetNode>(1);
        list.add(ConfigDescriptor.getLocalNode());
        return list;
    }

    @Override
    public List<NetNode> calculateReplicationNodes(TopologyMetaData metaData, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        return Collections.singletonList(ConfigDescriptor.getLocalNode());
    }
}
