/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.IDatabase;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.locator.AbstractNodeAssignmentStrategy;
import org.lealone.p2p.locator.AbstractReplicationStrategy;
import org.lealone.p2p.locator.TopologyMetaData;

public class P2pNetNodeManager implements NetNodeManager {

    private static final P2pNetNodeManager instance = new P2pNetNodeManager();

    private static final Map<IDatabase, AbstractReplicationStrategy> replicationStrategies = new HashMap<>();
    private static final AbstractReplicationStrategy defaultReplicationStrategy = ConfigDescriptor
            .getDefaultReplicationStrategy();

    private static final Map<IDatabase, AbstractNodeAssignmentStrategy> nodeAssignmentStrategies = new HashMap<>();
    private static final AbstractNodeAssignmentStrategy defaultNodeAssignmentStrategy = ConfigDescriptor
            .getDefaultNodeAssignmentStrategy();

    public static P2pNetNodeManager getInstance() {
        return instance;
    }

    protected P2pNetNodeManager() {
    }

    @Override
    public Set<NetNode> getLiveNodes() {
        return Gossiper.instance.getLiveMembers();
    }

    @Override
    public String[] assignNodes(IDatabase db) {
        HashSet<NetNode> oldNodes;
        List<NetNode> newNodes;

        String[] oldHostIds = db.getHostIds();
        if (oldHostIds == null) {
            oldNodes = new HashSet<>(0);
        } else {
            int size = oldHostIds.length;
            oldNodes = new HashSet<>(size);
            for (int i = 0; i < size; i++) {
                oldNodes.add(P2pServer.instance.getTopologyMetaData().getNode(oldHostIds[i]));
            }
        }

        removeNodeAssignmentStrategy(db); // 避免使用旧的
        newNodes = getNodeAssignmentStrategy(db).assignNodes(oldNodes, Gossiper.instance.getLiveMembers(), false);

        int size = newNodes.size();
        String[] hostIds = new String[size];
        int i = 0;
        for (NetNode e : newNodes) {
            String hostId = getHostId(e);
            if (hostId != null)
                hostIds[i] = hostId;
            i++;
        }
        return hostIds;
    }

    @Override
    public int getRpcTimeout() {
        return ConfigDescriptor.getRpcTimeout();
    }

    @Override
    public NetNode getNode(String hostId) {
        return P2pServer.instance.getTopologyMetaData().getNode(hostId);
    }

    @Override
    public Set<NetNode> getNodes(List<String> hostIds) {
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        Set<NetNode> nodes = new HashSet<>(hostIds.size());
        for (String hostId : hostIds) {
            nodes.add(md.getNode(hostId));
        }
        return nodes;
    }

    @Override
    public String getHostId(NetNode node) {
        return P2pServer.instance.getTopologyMetaData().getHostId(node);
    }

    @Override
    public List<NetNode> getReplicationNodes(IDatabase db, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes) {
        return getReplicationNodes(db, oldReplicationNodes, candidateNodes, false);
    }

    private static List<NetNode> getReplicationNodes(IDatabase db, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        return getReplicationStrategy(db).getReplicationNodes(P2pServer.instance.getTopologyMetaData(),
                oldReplicationNodes, candidateNodes, includeOldReplicationNodes);
    }

    private static AbstractReplicationStrategy getReplicationStrategy(IDatabase db) {
        if (db.getReplicationParameters() == null)
            return defaultReplicationStrategy;
        AbstractReplicationStrategy replicationStrategy = replicationStrategies.get(db);
        if (replicationStrategy == null) {
            CaseInsensitiveMap<String> map = new CaseInsensitiveMap<>(db.getReplicationParameters());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigException("Missing replication strategy class");
            }

            replicationStrategy = AbstractReplicationStrategy.create(db.getShortName(), className,
                    ConfigDescriptor.getNodeSnitch(), map, true);
            replicationStrategies.put(db, replicationStrategy);
        }
        return replicationStrategy;
    }

    private static void removeNodeAssignmentStrategy(IDatabase db) {
        nodeAssignmentStrategies.remove(db);
    }

    private static AbstractNodeAssignmentStrategy getNodeAssignmentStrategy(IDatabase db) {
        if (db.getNodeAssignmentParameters() == null)
            return defaultNodeAssignmentStrategy;
        AbstractNodeAssignmentStrategy nodeAssignmentStrategy = nodeAssignmentStrategies.get(db);
        if (nodeAssignmentStrategy == null) {
            CaseInsensitiveMap<String> map = new CaseInsensitiveMap<>(db.getNodeAssignmentParameters());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigException("Missing node assignment strategy class");
            }

            nodeAssignmentStrategy = AbstractNodeAssignmentStrategy.create(db.getShortName(), className,
                    ConfigDescriptor.getNodeSnitch(), map, true);
            nodeAssignmentStrategies.put(db, nodeAssignmentStrategy);
        }
        return nodeAssignmentStrategy;
    }

    @Override
    public Collection<String> getRecognizedReplicationStrategyOptions(String strategyName) {
        AbstractReplicationStrategy replicationStrategy;
        if (strategyName == null)
            replicationStrategy = defaultReplicationStrategy;
        else
            replicationStrategy = AbstractReplicationStrategy.create(null, strategyName,
                    ConfigDescriptor.getNodeSnitch(), null, false);
        return replicationStrategy.recognizedOptions();
    }

    @Override
    public Collection<String> getRecognizedNodeAssignmentStrategyOptions(String strategyName) {
        AbstractNodeAssignmentStrategy nodeAssignmentStrategy;
        if (strategyName == null)
            nodeAssignmentStrategy = defaultNodeAssignmentStrategy;
        else
            nodeAssignmentStrategy = AbstractNodeAssignmentStrategy.create(null, strategyName,
                    ConfigDescriptor.getNodeSnitch(), null, false);
        return nodeAssignmentStrategy.recognizedOptions();
    }

    @Override
    public String getDefaultReplicationStrategy() {
        return defaultReplicationStrategy.getClass().getSimpleName();
    }

    @Override
    public int getDefaultReplicationFactor() {
        return defaultReplicationStrategy.getReplicationFactor();
    }

    @Override
    public String getDefaultNodeAssignmentStrategy() {
        return defaultNodeAssignmentStrategy.getClass().getSimpleName();
    }

    @Override
    public int getDefaultNodeAssignmentFactor() {
        return defaultNodeAssignmentStrategy.getAssignmentFactor();
    }
}
