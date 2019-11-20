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
package org.lealone.p2p.net;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.net.NetNode;
import org.lealone.net.NetNodeManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.FailureDetector;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.locator.AbstractNodeAssignmentStrategy;
import org.lealone.p2p.locator.AbstractReplicationStrategy;
import org.lealone.p2p.locator.TopologyMetaData;
import org.lealone.p2p.server.P2pServer;
import org.lealone.storage.replication.ReplicationSession;

public class P2pNetNodeManager implements NetNodeManager {

    private static final P2pNetNodeManager instance = new P2pNetNodeManager();
    private static final Random random = new Random();

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
    public long getRpcTimeout() {
        return ConfigDescriptor.getRpcTimeout();
    }

    @Override
    public String[] assignNodes(IDatabase db) {
        removeNodeAssignmentStrategy(db); // 避免使用旧的
        List<NetNode> list = getNodeAssignmentStrategy(db).assignNodes(new HashSet<>(0),
                Gossiper.instance.getLiveMembers(), false);

        int size = list.size();
        String[] hostIds = new String[size];
        int i = 0;
        for (NetNode e : list) {
            String hostId = getHostId(e);
            if (hostId != null)
                hostIds[i] = hostId;
            i++;
        }
        return hostIds;
    }

    public String[] getHostIdsOld(IDatabase db) {
        RunMode runMode = db.getRunMode();
        Set<NetNode> liveMembers = Gossiper.instance.getLiveMembers();
        ArrayList<NetNode> list = new ArrayList<>(liveMembers);
        int size = liveMembers.size();
        if (runMode == RunMode.CLIENT_SERVER) {
            int i = random.nextInt(size);
            NetNode addr = list.get(i);
            return new String[] { getHostId(addr) };
        } else if (runMode == RunMode.REPLICATION) {
            AbstractReplicationStrategy replicationStrategy = getReplicationStrategy(db);
            int replicationFactor = replicationStrategy.getReplicationFactor();
            return getHostIds(list, size, replicationFactor);
        } else if (runMode == RunMode.SHARDING) {
            AbstractReplicationStrategy replicationStrategy = getReplicationStrategy(db);
            int replicationFactor = replicationStrategy.getReplicationFactor();
            Map<String, String> parameters = db.getParameters();
            int nodes = replicationFactor + 2;
            if (parameters != null && parameters.containsKey("nodes")) {
                nodes = Integer.parseInt(parameters.get("nodes"));
            }
            return getHostIds(list, size, nodes);
        }
        return new String[0];
    }

    private String[] getHostIds(ArrayList<NetNode> list, int totalNodes, int needNodes) {
        Set<Integer> indexSet = new HashSet<>(needNodes);
        if (needNodes >= totalNodes) {
            needNodes = totalNodes;
            for (int i = 0; i < totalNodes; i++) {
                indexSet.add(i);
            }
        } else {
            while (true) {
                int i = random.nextInt(totalNodes);
                indexSet.add(i);
                if (indexSet.size() == needNodes)
                    break;
            }
        }

        String[] hostIds = new String[needNodes];
        int j = 0;
        for (int i : indexSet) {
            String hostId = getHostId(list.get(i));
            if (hostId != null)
                hostIds[j++] = hostId;
        }

        return hostIds;
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes) {
        return createReplicationSession(session, replicationNodes, null);
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetNode> replicationNodes,
            Boolean remote) {
        NetNode localNode = ConfigDescriptor.getLocalNode();
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        int size = replicationNodes.size();
        Session[] sessions = new Session[size];
        int i = 0;
        for (NetNode e : replicationNodes) {
            String id = md.getHostId(e);
            sessions[i++] = session.getNestedSession(id, remote != null ? remote.booleanValue() : !localNode.equals(e));
        }
        return createReplicationSession(session, sessions);
    }

    public ReplicationSession createReplicationSession(Session s, List<String> replicationHostIds, Boolean remote) {
        Session session = s;
        NetNode localNode = NetNode.getLocalTcpNode();
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        Gossiper gossiper = Gossiper.instance;
        int size = replicationHostIds.size();
        Session[] sessions = new Session[size];
        int i = 0;
        for (String hostId : replicationHostIds) {
            NetNode p2pNode = md.getNode(hostId);
            NetNode tcpNode = gossiper.getTcpNode(p2pNode);
            sessions[i++] = session.getNestedSession(tcpNode.getHostAndPort(),
                    remote != null ? remote.booleanValue() : !localNode.equals(tcpNode));
        }
        return createReplicationSession(session, sessions);
    }

    @Override
    public ReplicationSession createReplicationSession(Session s, Session[] sessions) {
        ReplicationSession rs = new ReplicationSession(sessions);
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
        rs.setAutoCommit(s.isAutoCommit());
        rs.setParentTransaction(s.getTransaction());
        return rs;
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
    public String[] getReplicationNodes(IDatabase db) {
        removeReplicationStrategy(db); // 避免使用旧的
        String[] oldHostIds = db.getHostIds();
        int size = oldHostIds.length;
        List<NetNode> oldReplicationNodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            oldReplicationNodes.add(P2pServer.instance.getTopologyMetaData().getNode(oldHostIds[i]));
        }
        List<NetNode> newReplicationNodes = getLiveReplicationNodes(db, new HashSet<>(oldReplicationNodes),
                Gossiper.instance.getLiveMembers(), true);

        size = newReplicationNodes.size();
        String[] hostIds = new String[size];
        int j = 0;
        for (NetNode e : newReplicationNodes) {
            String hostId = getHostId(e);
            if (hostId != null)
                hostIds[j++] = hostId;
        }
        return hostIds;
    }

    @Override
    public String[] getShardingNodes(IDatabase db) {
        HashSet<NetNode> oldNodes = new HashSet<>();
        for (String hostId : db.getHostIds()) {
            oldNodes.add(P2pServer.instance.getTopologyMetaData().getNode(hostId));
        }
        Set<NetNode> liveMembers = Gossiper.instance.getLiveMembers();
        liveMembers.removeAll(oldNodes);
        ArrayList<NetNode> list = new ArrayList<>(liveMembers);
        int size = liveMembers.size();
        AbstractReplicationStrategy replicationStrategy = getReplicationStrategy(db);
        int replicationFactor = replicationStrategy.getReplicationFactor();
        Map<String, String> parameters = db.getParameters();
        int nodes = replicationFactor + 2;
        if (parameters != null && parameters.containsKey("nodes")) {
            nodes = Integer.parseInt(parameters.get("nodes"));
        }
        nodes -= db.getHostIds().length;
        return getHostIds(list, size, nodes);
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
