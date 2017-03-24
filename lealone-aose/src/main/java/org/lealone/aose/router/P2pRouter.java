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
package org.lealone.aose.router;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.Gossiper;
import org.lealone.aose.locator.AbstractReplicationStrategy;
import org.lealone.aose.locator.TopologyMetaData;
import org.lealone.aose.server.ClusterMetaData;
import org.lealone.aose.server.P2pServer;
import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Command;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.db.Session;
import org.lealone.db.result.Result;
import org.lealone.net.NetEndpoint;
import org.lealone.replication.ReplicationSession;
import org.lealone.sql.StatementBase;
import org.lealone.sql.ddl.DatabaseStatement;
import org.lealone.sql.ddl.DefineStatement;
import org.lealone.sql.router.Router;

public class P2pRouter implements Router {

    private static final P2pRouter INSTANCE = new P2pRouter();
    private static final Random random = new Random();

    public static P2pRouter getInstance() {
        return INSTANCE;
    }

    protected P2pRouter() {
    }

    private int executeDefineStatement(DefineStatement defineStatement) {
        Set<NetEndpoint> liveMembers;
        ServerSession currentSession = defineStatement.getSession();
        Database db = currentSession.getDatabase();
        if (defineStatement instanceof DatabaseStatement) {
            // TODO 需要细分哪些DatabaseStatement语句可以让普通用户执行
            if (db == LealoneDatabase.getInstance()) {
                liveMembers = Gossiper.instance.getLiveMembers();
            } else {
                // 生成合适的错误代码，只有用LealoneDatabase中的用户才能执行create/alter/drop database语句
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "create/alter/drop database only allowed for the super user");
            }
        } else {
            String[] hostIds = db.getHostIds();
            if (hostIds.length == 0) {
                throw DbException
                        .throwInternalError("DB: " + db.getName() + ", Run Mode: " + db.getRunMode() + ", no hostIds");
            } else {
                liveMembers = new HashSet<>(hostIds.length);
                TopologyMetaData metaData = P2pServer.instance.getTopologyMetaData();
                for (String hostId : hostIds) {
                    liveMembers.add(metaData.getEndpointForHostId(hostId));
                }
            }
        }
        List<String> initReplicationEndpoints = null;
        // 在sharding模式下执行ReplicationStatement时，需要预先为root page初始化默认的复制节点
        if (defineStatement.isReplicationStatement() && db.isShardingMode() && !db.isStarting()) {
            List<NetEndpoint> endpoints = P2pServer.instance.getReplicationEndpoints(db,
                    P2pServer.instance.getLocalHostId(), liveMembers);
            if (!endpoints.isEmpty()) {
                initReplicationEndpoints = new ArrayList<>(endpoints.size());
                for (NetEndpoint e : endpoints) {
                    String hostId = P2pServer.instance.getTopologyMetaData().getHostId(e);
                    initReplicationEndpoints.add(hostId);
                }
            }
        }

        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (NetEndpoint e : liveMembers) {
            String hostId = P2pServer.instance.getTopologyMetaData().getHostId(e);
            sessions[i++] = currentSession.getNestedSession(hostId, !ConfigDescriptor.getLocalEndpoint().equals(e));
        }

        ReplicationSession rs = new ReplicationSession(sessions, initReplicationEndpoints);
        rs.setAutoCommit(currentSession.isAutoCommit());
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
        Command c = null;
        try {
            c = rs.createCommand(defineStatement.getSQL(), -1);
            return c.executeUpdate();
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

    @Override
    public int executeUpdate(StatementBase statement) {
        if ((statement instanceof DefineStatement) && !statement.isLocal()) {
            return executeDefineStatement((DefineStatement) statement);
        }
        return statement.executeUpdate();
    }

    @Override
    public Result executeQuery(StatementBase statement, int maxRows) {
        return statement.executeQuery(maxRows);
    }

    @Override
    public String[] getHostIds(Database db) {
        RunMode runMode = db.getRunMode();
        Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
        ArrayList<NetEndpoint> list = new ArrayList<>(liveMembers);
        int size = liveMembers.size();
        if (runMode == RunMode.CLIENT_SERVER) {
            int i = random.nextInt(size);
            NetEndpoint addr = list.get(i);
            return new String[] { P2pServer.instance.getTopologyMetaData().getHostId(addr) };
        } else if (runMode == RunMode.REPLICATION) {
            AbstractReplicationStrategy replicationStrategy = ClusterMetaData.getReplicationStrategy(db);
            int replicationFactor = replicationStrategy.getReplicationFactor();
            return getHostIds(list, size, replicationFactor);
        } else if (runMode == RunMode.SHARDING) {
            AbstractReplicationStrategy replicationStrategy = ClusterMetaData.getReplicationStrategy(db);
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
        for (int i : indexSet) {
            String hostId = P2pServer.instance.getTopologyMetaData().getHostId(list.get(i));
            if (hostId != null)
                hostIds[i] = hostId;
        }

        return hostIds;
    }

    @Override
    public int createDatabase(Database db, ServerSession currentSession) {
        Set<NetEndpoint> liveMembers = Gossiper.instance.getLiveMembers();
        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (NetEndpoint e : liveMembers) {
            String hostId = P2pServer.instance.getTopologyMetaData().getHostId(e);
            boolean isLocal = ConfigDescriptor.getLocalEndpoint().equals(e);
            sessions[i] = currentSession.getNestedSession(hostId, !isLocal);
            if (isLocal) {
                currentSession.copyLastReplicationStatusTo((ServerSession) sessions[i]);
            }
            i++;
        }

        ReplicationSession rs = new ReplicationSession(sessions);
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
        rs.setAutoCommit(currentSession.isAutoCommit());
        Command c = null;
        try {
            c = rs.createCommand(db.getCreateSQL(), -1);
            return c.executeUpdate();
        } catch (Exception e) {
            throw DbException.convert(e);
        } finally {
            if (c != null)
                c.close();
        }
    }

}
