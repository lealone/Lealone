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
import org.lealone.db.IDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetEndpointManager;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.FailureDetector;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.locator.AbstractEndpointAssignmentStrategy;
import org.lealone.p2p.locator.AbstractReplicationStrategy;
import org.lealone.p2p.locator.TopologyMetaData;
import org.lealone.p2p.server.P2pServer;
import org.lealone.storage.replication.ReplicationSession;

public class P2pNetEndpointManager implements NetEndpointManager {

    private static final P2pNetEndpointManager instance = new P2pNetEndpointManager();
    private static final Random random = new Random();

    private static final Map<IDatabase, AbstractReplicationStrategy> replicationStrategies = new HashMap<>();
    private static final AbstractReplicationStrategy defaultReplicationStrategy = ConfigDescriptor
            .getDefaultReplicationStrategy();

    private static final Map<IDatabase, AbstractEndpointAssignmentStrategy> endpointAssignmentStrategies = new HashMap<>();
    private static final AbstractEndpointAssignmentStrategy defaultEndpointAssignmentStrategy = ConfigDescriptor
            .getDefaultEndpointAssignmentStrategy();

    public static P2pNetEndpointManager getInstance() {
        return instance;
    }

    protected P2pNetEndpointManager() {
    }

    @Override
    public Set<NetEndpoint> getLiveEndpoints() {
        return Gossiper.instance.getLiveMembers();
    }

    @Override
    public long getRpcTimeout() {
        return ConfigDescriptor.getRpcTimeout();
    }

    @Override
    public String[] assignEndpoints(IDatabase db) {
        removeEndpointAssignmentStrategy(db); // 避免使用旧的
        List<NetEndpoint> list = getEndpointAssignmentStrategy(db).assignEndpoints(new HashSet<>(0),
                Gossiper.instance.getLiveMembers(), false);

        int size = list.size();
        String[] hostIds = new String[size];
        int i = 0;
        for (NetEndpoint e : list) {
            String hostId = getHostId(e);
            if (hostId != null)
                hostIds[i] = hostId;
            i++;
        }
        return hostIds;
    }

    public String[] getHostIdsOld(IDatabase db) {
        RunMode runMode = db.getRunMode();
        Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
        ArrayList<NetEndpoint> list = new ArrayList<>(liveMembers);
        int size = liveMembers.size();
        if (runMode == RunMode.CLIENT_SERVER) {
            int i = random.nextInt(size);
            NetEndpoint addr = list.get(i);
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

    private String[] getHostIds(ArrayList<NetEndpoint> list, int totalNodes, int needNodes) {
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
    public ReplicationSession createReplicationSession(Session session, Collection<NetEndpoint> replicationEndpoints) {
        return createReplicationSession(session, replicationEndpoints, null);
    }

    @Override
    public ReplicationSession createReplicationSession(Session session, Collection<NetEndpoint> replicationEndpoints,
            Boolean remote) {
        NetEndpoint localEndpoint = ConfigDescriptor.getLocalEndpoint();
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        int size = replicationEndpoints.size();
        Session[] sessions = new Session[size];
        int i = 0;
        for (NetEndpoint e : replicationEndpoints) {
            String id = md.getHostId(e);
            sessions[i++] = session.getNestedSession(id,
                    remote != null ? remote.booleanValue() : !localEndpoint.equals(e));
        }
        return createReplicationSession(session, sessions);
    }

    public ReplicationSession createReplicationSession(Session s, List<String> replicationHostIds, Boolean remote) {
        Session session = s;
        NetEndpoint localEndpoint = NetEndpoint.getLocalTcpEndpoint();
        TopologyMetaData md = P2pServer.instance.getTopologyMetaData();
        Gossiper gossiper = Gossiper.instance;
        int size = replicationHostIds.size();
        Session[] sessions = new Session[size];
        int i = 0;
        for (String hostId : replicationHostIds) {
            NetEndpoint p2pEndpoint = md.getEndpoint(hostId);
            NetEndpoint tcpEndpoint = gossiper.getTcpEndpoint(p2pEndpoint);
            sessions[i++] = session.getNestedSession(tcpEndpoint.getHostAndPort(),
                    remote != null ? remote.booleanValue() : !localEndpoint.equals(tcpEndpoint));
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
    public NetEndpoint getEndpoint(String hostId) {
        return P2pServer.instance.getTopologyMetaData().getEndpoint(hostId);
    }

    @Override
    public String getHostId(NetEndpoint endpoint) {
        return P2pServer.instance.getTopologyMetaData().getHostId(endpoint);
    }

    @Override
    public String[] getReplicationEndpoints(IDatabase db) {
        removeReplicationStrategy(db); // 避免使用旧的
        String[] oldHostIds = db.getHostIds();
        int size = oldHostIds.length;
        List<NetEndpoint> oldReplicationEndpoints = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            oldReplicationEndpoints.add(P2pServer.instance.getTopologyMetaData().getEndpoint(oldHostIds[i]));
        }
        List<NetEndpoint> newReplicationEndpoints = getLiveReplicationEndpoints(db,
                new HashSet<>(oldReplicationEndpoints), Gossiper.instance.getLiveMembers(), true);

        size = newReplicationEndpoints.size();
        String[] hostIds = new String[size];
        int j = 0;
        for (NetEndpoint e : newReplicationEndpoints) {
            String hostId = getHostId(e);
            if (hostId != null)
                hostIds[j++] = hostId;
        }
        return hostIds;
    }

    @Override
    public String[] getShardingEndpoints(IDatabase db) {
        HashSet<NetEndpoint> oldEndpoints = new HashSet<>();
        for (String hostId : db.getHostIds()) {
            oldEndpoints.add(P2pServer.instance.getTopologyMetaData().getEndpoint(hostId));
        }
        Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
        liveMembers.removeAll(oldEndpoints);
        ArrayList<NetEndpoint> list = new ArrayList<>(liveMembers);
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
    public List<NetEndpoint> getReplicationEndpoints(IDatabase db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints) {
        return getReplicationEndpoints(db, oldReplicationEndpoints, candidateEndpoints, false);
    }

    private static List<NetEndpoint> getReplicationEndpoints(IDatabase db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints, boolean includeOldReplicationEndpoints) {
        return getReplicationStrategy(db).getReplicationEndpoints(P2pServer.instance.getTopologyMetaData(),
                oldReplicationEndpoints, candidateEndpoints, includeOldReplicationEndpoints);
    }

    private static List<NetEndpoint> getLiveReplicationEndpoints(IDatabase db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints, boolean includeOldReplicationEndpoints) {
        List<NetEndpoint> endpoints = getReplicationEndpoints(db, oldReplicationEndpoints, candidateEndpoints,
                includeOldReplicationEndpoints);
        List<NetEndpoint> liveEps = new ArrayList<>(endpoints.size());
        for (NetEndpoint endpoint : endpoints) {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }
        return liveEps;
    }

    private static void removeReplicationStrategy(IDatabase db) {
        replicationStrategies.remove(db);
    }

    private static AbstractReplicationStrategy getReplicationStrategy(IDatabase db) {
        if (db.getReplicationProperties() == null)
            return defaultReplicationStrategy;
        AbstractReplicationStrategy replicationStrategy = replicationStrategies.get(db);
        if (replicationStrategy == null) {
            HashMap<String, String> map = new HashMap<>(db.getReplicationProperties());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigException("Missing replication strategy class");
            }

            replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(db.getShortName(), className,
                    ConfigDescriptor.getEndpointSnitch(), map);
            replicationStrategies.put(db, replicationStrategy);
        }
        return replicationStrategy;
    }

    private static void removeEndpointAssignmentStrategy(IDatabase db) {
        endpointAssignmentStrategies.remove(db);
    }

    private static AbstractEndpointAssignmentStrategy getEndpointAssignmentStrategy(IDatabase db) {
        if (db.getEndpointAssignmentProperties() == null)
            return defaultEndpointAssignmentStrategy;
        AbstractEndpointAssignmentStrategy endpointAssignmentStrategy = endpointAssignmentStrategies.get(db);
        if (endpointAssignmentStrategy == null) {
            HashMap<String, String> map = new HashMap<>(db.getEndpointAssignmentProperties());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigException("Missing endpoint assignment strategy class");
            }

            endpointAssignmentStrategy = AbstractEndpointAssignmentStrategy.create(db.getShortName(), className,
                    ConfigDescriptor.getEndpointSnitch(), map);
            endpointAssignmentStrategies.put(db, endpointAssignmentStrategy);
        }
        return endpointAssignmentStrategy;
    }
}
