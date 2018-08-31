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

    public static enum BootstrapState {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetaData.class);
    private static final String LOCAL_TABLE = "local";
    private static final String PEERS_TABLE = "peers";
    private static final String LOCAL_KEY = "local";

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
            stmt.execute("CREATE TABLE IF NOT EXISTS " + PEERS_TABLE + "(" //
                    + "peer varchar,"//
                    + "data_center varchar,"//
                    + "host_id varchar,"//
                    + "preferred_ip varchar,"//
                    + "rack varchar,"//
                    + "release_version varchar,"//
                    + "tcp_endpoint varchar,"//
                    + "p2p_endpoint varchar,"//
                    + "schema_version uuid,"//
                    + "PRIMARY KEY (peer))");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + LOCAL_TABLE + "("//
                    + "key varchar,"//
                    + "bootstrapped varchar,"//
                    + "cluster_name varchar,"//
                    + "cql_version varchar,"//
                    + "data_center varchar,"//
                    + "gossip_generation int,"//
                    + "host_id varchar,"//
                    + "native_protocol_version varchar,"//
                    + "partitioner varchar,"//
                    + "rack varchar,"//
                    + "release_version varchar,"//
                    + "schema_version uuid,"//
                    + "PRIMARY KEY (key))");
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleException(Exception e) {
        // TODO 是否要重新抛出异常
        logger.error("Cluster metadata exception", e);
    }

    public static Map<NetEndpoint, Map<String, String>> loadDcRackInfo() {
        return null;
    }

    public static NetEndpoint getPreferredIP(NetEndpoint ep) {
        return ep;
    }

    public static synchronized void updatePreferredIP(NetEndpoint ep, NetEndpoint preferred_ip) {
    }

    public static BootstrapState getBootstrapState() {
        String sql = "SELECT bootstrapped FROM %s WHERE key='%s'";
        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, LOCAL_TABLE, LOCAL_KEY));
            if (rs.next()) {
                String bootstrapped = rs.getString(1);
                if (bootstrapped != null) {
                    rs.close();
                    return BootstrapState.valueOf(bootstrapped);
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }

        return BootstrapState.NEEDS_BOOTSTRAP;
    }

    public static boolean bootstrapComplete() {
        return getBootstrapState() == BootstrapState.COMPLETED;
    }

    public static boolean bootstrapInProgress() {
        return getBootstrapState() == BootstrapState.IN_PROGRESS;
    }

    public static void setBootstrapState(BootstrapState state) {
        // String sql = "INSERT INTO %s (key, bootstrapped) VALUES ('%s', '%s')";
        String sql = "UPDATE %s SET bootstrapped = '%s' WHERE key = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, LOCAL_TABLE, state.name(), LOCAL_KEY));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static Map<NetEndpoint, String> loadHostIds() {
        Map<NetEndpoint, String> hostIdMap = new HashMap<>();
        try {
            ResultSet rs = stmt.executeQuery("SELECT peer, host_id FROM " + PEERS_TABLE);
            while (rs.next()) {
                String hostId = rs.getString(2);
                NetEndpoint peer = NetEndpoint.getByName(rs.getString(1));
                hostIdMap.put(peer, hostId);
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }
        return hostIdMap;
    }

    public static String getLocalHostId() {
        String sql = "SELECT host_id FROM %s WHERE key='%s'";

        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, LOCAL_TABLE, LOCAL_KEY));
            if (rs.next()) {
                String hostId = rs.getString(1);
                rs.close();
                return hostId;
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }

        String hostId = NetEndpoint.getLocalTcpEndpoint().getHostAndPort();
        return setLocalHostId(hostId);
    }

    public static String setLocalHostId(String hostId) {
        String sql = "INSERT INTO %s (key, host_id) VALUES ('%s', '%s')";
        try {
            stmt.executeUpdate(String.format(sql, LOCAL_TABLE, LOCAL_KEY, hostId.toString()));
        } catch (SQLException e) {
            handleException(e);
        }

        return hostId;
    }

    public static int incrementAndGetGeneration() {
        String sql = "SELECT gossip_generation FROM %s WHERE key='%s'";
        int generation = 0;
        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, LOCAL_TABLE, LOCAL_KEY));
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
        sql = "UPDATE %s SET gossip_generation = %d WHERE key = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, LOCAL_TABLE, generation, LOCAL_KEY));
        } catch (SQLException e) {
            handleException(e);
        }

        return generation;
    }

    // 由调用者确定是否把本地节点的信息存入PEERS表
    public static synchronized void updatePeerInfo(NetEndpoint ep, String columnName, Object value) {
        String sql = "MERGE INTO %s (peer, %s) KEY(peer) VALUES('%s', '%s')";
        try {
            stmt.executeUpdate(String.format(sql, PEERS_TABLE, columnName, ep.getHostAndPort(), value));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static synchronized void removeEndpoint(NetEndpoint ep) {
        String sql = "DELETE FROM %s WHERE peer = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, PEERS_TABLE, ep.getHostAddress()));
        } catch (SQLException e) {
            handleException(e);
        }
    }

}
