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
package org.lealone.aose.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.locator.AbstractReplicationStrategy;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.net.NetEndpoint;

public class ClusterMetaData {

    private ClusterMetaData() {
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetaData.class);
    private static final String NODES_TABLE = "nodes";

    private static final HashMap<Database, AbstractReplicationStrategy> replicationStrategys = new HashMap<>();
    private static final AbstractReplicationStrategy defaultReplicationStrategy = ConfigDescriptor
            .getDefaultReplicationStrategy();

    public static void removeReplicationStrategy(Database db) {
        replicationStrategys.remove(db);
    }

    public static AbstractReplicationStrategy getReplicationStrategy(Database db) {
        if (db.getReplicationProperties() == null)
            return defaultReplicationStrategy;
        AbstractReplicationStrategy replicationStrategy = replicationStrategys.get(db);
        if (replicationStrategy == null) {
            HashMap<String, String> map = new HashMap<>(db.getReplicationProperties());
            String className = map.remove("class");
            if (className == null) {
                throw new ConfigException("Missing replication strategy class");
            }

            replicationStrategy = AbstractReplicationStrategy.createReplicationStrategy(db.getName(),
                    AbstractReplicationStrategy.getClass(className), P2pServer.instance.getTopologyMetaData(),
                    ConfigDescriptor.getEndpointSnitch(), map);
            replicationStrategys.put(db, replicationStrategy);
        }
        return replicationStrategy;
    }

    private static Connection conn;
    private static Statement stmt;

    static {
        try {
            conn = LealoneDatabase.getInstance().getInternalConnection();
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS " + NODES_TABLE + "(" //
                    + "id varchar,"//
                    + "host_id varchar,"//
                    + "tcp_endpoint varchar,"//
                    + "p2p_endpoint varchar,"//
                    + "data_center varchar,"//
                    + "rack varchar,"//
                    + "release_version varchar,"//
                    + "net_version varchar,"//
                    + "preferred_ip varchar,"//
                    + "gossip_generation int,"//
                    + "schema_version uuid,"//
                    + "PRIMARY KEY (id))");
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleException(Exception e) {
        // TODO 是否要重新抛出异常
        logger.error("Cluster metadata exception", e);
    }

    public static synchronized Map<NetEndpoint, String> loadHostIds() {
        Map<NetEndpoint, String> hostIdMap = new HashMap<>();
        try {
            ResultSet rs = stmt.executeQuery("SELECT host_id, p2p_endpoint FROM " + NODES_TABLE);
            while (rs.next()) {
                String hostId = rs.getString(1);
                String p2pEndpoint = rs.getString(2);
                if (p2pEndpoint == null) {
                    continue;
                }
                NetEndpoint endpoint = NetEndpoint.getByName(p2pEndpoint);
                hostIdMap.put(endpoint, hostId);
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }
        return hostIdMap;
    }

    public static synchronized int incrementAndGetGeneration(NetEndpoint ep) {
        String sql = "SELECT gossip_generation FROM %s WHERE id='%s'";
        int generation = 0;
        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, NODES_TABLE, ep.getHostAndPort()));
            if (rs.next()) {
                generation = rs.getInt(1);
                if (generation == 0) {
                    // seconds-since-epoch isn't a foolproof new generation
                    // (where foolproof is "guaranteed to be larger than the last one seen at this ip address"),
                    // but it's as close as sanely possible
                    generation = (int) (System.currentTimeMillis() / 1000);
                } else {
                    // Other nodes will ignore gossip messages about a node that have a lower generation than previously
                    // seen.
                    final int storedGeneration = generation + 1;
                    final int now = (int) (System.currentTimeMillis() / 1000);
                    if (storedGeneration >= now) {
                        logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  "
                                + "See CASSANDRA-3654 if you experience problems", storedGeneration, now);
                        generation = storedGeneration;
                    } else {
                        generation = now;
                    }
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }

        updatePeerInfo(ep, "gossip_generation", generation);
        return generation;
    }

    // 由调用者确定是否把本地节点的信息存入NODES表
    public static synchronized void updatePeerInfo(NetEndpoint ep, String columnName, Object value) {
        try {
            String sql = "MERGE INTO %s (id, %s) KEY(id) VALUES('%s', '%s')";
            stmt.executeUpdate(String.format(sql, NODES_TABLE, columnName, ep.getHostAndPort(), value));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static synchronized void removeEndpoint(NetEndpoint ep) {
        String sql = "DELETE FROM %s WHERE id = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, NODES_TABLE, ep.getHostAndPort()));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static synchronized Map<NetEndpoint, Map<String, String>> loadDcRackInfo() {
        Map<NetEndpoint, Map<String, String>> map = new HashMap<>();
        try {
            ResultSet rs = stmt.executeQuery("SELECT host_id, data_center, rack FROM " + NODES_TABLE);
            while (rs.next()) {
                NetEndpoint endpoint = NetEndpoint.getByName(rs.getString(1));
                Map<String, String> dcRackInfo = new HashMap<>();
                dcRackInfo.put("data_center", rs.getString(2));
                dcRackInfo.put("rack", rs.getString(3));
                map.put(endpoint, dcRackInfo);
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }
        return map;
    }

    public static synchronized NetEndpoint getPreferredIP(NetEndpoint ep) {
        String sql = "SELECT preferred_ip FROM %s WHERE id='%s'";
        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, NODES_TABLE, ep.getHostAndPort()));
            if (rs.next()) {
                String preferredIp = rs.getString(1);
                if (preferredIp != null)
                    return NetEndpoint.getByName(preferredIp);
            }
        } catch (Exception e) {
            handleException(e);
        }
        return ep;
    }

    public static synchronized void updatePreferredIP(NetEndpoint ep, NetEndpoint preferred_ip) {
        updatePeerInfo(ep, "preferred_ip", preferred_ip.getHostAndPort());
    }
}
