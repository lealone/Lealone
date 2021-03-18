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
package org.lealone.p2p.server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gossip.FailureDetector;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.locator.AbstractNodeAssignmentStrategy;
import org.lealone.p2p.locator.AbstractReplicationStrategy;

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

        // 复制模式只使用ReplicationStrategy来分配所需的节点，其他模式用NodeAssignmentStrategy
        if (db.getRunMode() == RunMode.REPLICATION) {
            removeReplicationStrategy(db); // 避免使用旧的
            newNodes = getLiveReplicationNodes(db, oldNodes, Gossiper.instance.getLiveMembers(), true);
        } else {
            removeNodeAssignmentStrategy(db); // 避免使用旧的
            newNodes = getNodeAssignmentStrategy(db).assignNodes(oldNodes, Gossiper.instance.getLiveMembers(), false);
        }

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
    public long getRpcTimeout() {
        return ConfigDescriptor.getRpcTimeout();
    }

    @Override
    public NetNode getNode(String hostId) {
        return P2pServer.instance.getTopologyMetaData().getNode(hostId);
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

    private static List<NetNode> getLiveReplicationNodes(IDatabase db, Set<NetNode> oldReplicationNodes,
            Set<NetNode> candidateNodes, boolean includeOldReplicationNodes) {
        List<NetNode> nodes = getReplicationNodes(db, oldReplicationNodes, candidateNodes, includeOldReplicationNodes);
        List<NetNode> liveEps = new ArrayList<>(nodes.size());
        for (NetNode node : nodes) {
            if (FailureDetector.instance.isAlive(node))
                liveEps.add(node);
        }
        return liveEps;
    }

    private static void removeReplicationStrategy(IDatabase db) {
        replicationStrategies.remove(db);
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
