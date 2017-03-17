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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.Gossiper;
import org.lealone.aose.locator.AbstractReplicationStrategy;
import org.lealone.aose.server.ClusterMetaData;
import org.lealone.aose.server.P2PServer;
import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Command;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.RunMode;
import org.lealone.db.ServerSession;
import org.lealone.db.Session;
import org.lealone.db.SessionPool;
import org.lealone.db.result.Result;
import org.lealone.replication.ReplicationSession;
import org.lealone.sql.StatementBase;
import org.lealone.sql.ddl.DatabaseStatement;
import org.lealone.sql.ddl.DefineStatement;
import org.lealone.sql.router.Router;

public class P2PRouter implements Router {

    private static final P2PRouter INSTANCE = new P2PRouter();

    public static P2PRouter getInstance() {
        return INSTANCE;
    }

    protected P2PRouter() {
    }

    private int executeDefineStatement(DefineStatement defineStatement) {
        Set<InetAddress> liveMembers;
        ServerSession s = defineStatement.getSession();
        Database db = s.getDatabase();
        if (defineStatement instanceof DatabaseStatement) {
            if (db == LealoneDatabase.getInstance()) {
                liveMembers = Gossiper.instance.getLiveMembers();
            } else {
                // 生成合适的错误代码，只有用LealoneDatabase中的用户才能执行create/alter/drop database语句
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "create/alter/drop database only allowed for the super user");
            }
        } else {
            int[] hostIds = db.getHostIds();
            if (hostIds.length == 0) {
                liveMembers = Gossiper.instance.getLiveMembers();
            } else {
                liveMembers = new HashSet<>(hostIds.length);
                for (int hostId : hostIds) {
                    liveMembers.add(P2PServer.instance.getTopologyMetaData().getEndpointForHostId(hostId));
                }
            }
        }

        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (InetAddress ia : liveMembers)
            sessions[i++] = SessionPool.getSession(s, s.getURL(ia), !ConfigDescriptor.getLocalAddress().equals(ia));

        ReplicationSession rs = new ReplicationSession(sessions);
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
        if (statement instanceof DefineStatement) {
            if (statement.isLocal())
                return statement.executeUpdate();
            else
                return executeDefineStatement((DefineStatement) statement);
        }
        return statement.executeUpdate();
    }

    @Override
    public Result executeQuery(StatementBase statement, int maxRows) {
        return statement.executeQuery(maxRows);
    }

    private final static Random random = new Random();

    @Override
    public int[] getHostIds(Database db) {
        RunMode runMode = db.getRunMode();
        // Map<String, String> parameters;
        Set<InetAddress> liveMembers = Gossiper.instance.getLiveMembers();
        ArrayList<InetAddress> list = new ArrayList<>(liveMembers);
        int size = liveMembers.size();
        if (runMode == RunMode.CLIENT_SERVER) {
            int i = random.nextInt(size);
            InetAddress addr = list.get(i);
            return new int[] { P2PServer.instance.getTopologyMetaData().getHostId(addr) };
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
        return new int[0];
    }

    private int[] getHostIds(ArrayList<InetAddress> list, int liveNodes, int replicationNodes) {
        if (replicationNodes > liveNodes)
            replicationNodes = liveNodes;
        Set<Integer> indexSet = new HashSet<>(replicationNodes);
        while (true) {
            int i = random.nextInt(liveNodes);
            indexSet.add(i);
            if (indexSet.size() == replicationNodes)
                break;
        }

        int[] hostIds = new int[replicationNodes];
        for (int i : indexSet) {
            Integer hostId = P2PServer.instance.getTopologyMetaData().getHostId(list.get(i));
            if (hostId != null)
                hostIds[i] = hostId.intValue();
        }

        return hostIds;
    }

    @Override
    public int createDatabase(Database db, ServerSession currentSession) {
        Set<InetAddress> liveMembers = Gossiper.instance.getLiveMembers();
        // liveMembers.remove(Utils.getBroadcastAddress()); // TODO 要不要删除当前节点
        // int[] hostIds = db.getHostIds();
        // if (hostIds.length == 0) {
        // liveMembers = Gossiper.instance.getLiveMembers();
        // } else {
        // liveMembers = new HashSet<>(hostIds.length);
        // for (int hostId : hostIds) {
        // liveMembers.add(StorageServer.instance.getTopologyMetaData().getEndpointForHostId(hostId));
        // }
        // }
        Session[] sessions = new Session[liveMembers.size()];
        int i = 0;
        for (InetAddress ia : liveMembers) {
            sessions[i++] = SessionPool.getSession(currentSession, currentSession.getURL(ia),
                    !ConfigDescriptor.getLocalAddress().equals(ia));
        }

        ReplicationSession rs = new ReplicationSession(sessions);
        rs.setRpcTimeout(ConfigDescriptor.getRpcTimeout());
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

    @Override
    public String[] getEndpoints(Database db) {
        Set<InetAddress> liveMembers;
        int[] hostIds = db.getHostIds();
        if (hostIds.length == 0) {
            liveMembers = Gossiper.instance.getLiveMembers();
        } else {
            liveMembers = new HashSet<>(hostIds.length);
            for (int hostId : hostIds) {
                liveMembers.add(P2PServer.instance.getTopologyMetaData().getEndpointForHostId(hostId));
            }
        }

        String[] endpoints = new String[liveMembers.size()];
        int i = 0;
        for (InetAddress inetAddress : liveMembers) {
            // TODO 如何不用默认端口？
            endpoints[i++] = inetAddress.getHostAddress() + ":" + Constants.DEFAULT_TCP_PORT;
        }
        return endpoints;
    }

}
