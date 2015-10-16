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
package org.lealone.cluster.db;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.lealone.client.jdbc.Driver;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.dht.TokenFactory;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.utils.Utils;
import org.lealone.db.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;

public class ClusterMetaData {
    public static final String CLUSTER_DB_NAME = Constants.PROJECT_NAME + Constants.NAME_SEPARATOR + "cluster";

    public static enum BootstrapState {
        NEEDS_BOOTSTRAP,
        COMPLETED,
        IN_PROGRESS
    }

    private static final Logger logger = LoggerFactory.getLogger(ClusterMetaData.class);
    private static final String LOCAL_TABLE = "local";
    private static final String PEERS_TABLE = "peers";
    private static final String LOCAL_KEY = "local";

    private static Connection conn;
    private static Statement stmt;

    static {
        try {
            String url = Constants.URL_PREFIX + Constants.URL_EMBED + CLUSTER_DB_NAME;
            conn = Driver.getInternalConnection(url, CLUSTER_DB_NAME, CLUSTER_DB_NAME);
            stmt = conn.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS " + PEERS_TABLE + "(" //
                    + "peer varchar,"//
                    + "data_center varchar,"//
                    + "host_id uuid,"//
                    + "preferred_ip varchar,"//
                    + "rack varchar,"//
                    + "release_version varchar,"//
                    + "rpc_address varchar,"//
                    + "schema_version uuid,"//
                    + "tokens varchar,"//
                    + "PRIMARY KEY (peer))");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + LOCAL_TABLE + "("//
                    + "key varchar,"//
                    + "bootstrapped varchar,"//
                    + "cluster_name varchar,"//
                    + "cql_version varchar,"//
                    + "data_center varchar,"//
                    + "gossip_generation int,"//
                    + "host_id uuid,"//
                    + "native_protocol_version varchar,"//
                    + "partitioner varchar,"//
                    + "rack varchar,"//
                    + "release_version varchar,"//
                    + "schema_version uuid,"//
                    + "tokens varchar,"//
                    // + "truncated_at map<uuid, blob>,"//
                    + "PRIMARY KEY (key))");
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static void handleException(Exception e) {
        // TODO 是否要重新抛出异常
        logger.error("Cluster metadata exception", e);
    }

    public static Map<InetAddress, Map<String, String>> loadDcRackInfo() {
        return null;
    }

    public static InetAddress getPreferredIP(InetAddress ep) {
        return ep;
    }

    public static synchronized void updatePreferredIP(InetAddress ep, InetAddress preferred_ip) {

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

    private static Collection<Token> deserializeTokens(Collection<String> tokensStrings) {
        TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = new ArrayList<>(tokensStrings.size());
        for (String tk : tokensStrings)
            tokens.add(factory.fromString(tk));
        return tokens;
    }

    public static SetMultimap<InetAddress, Token> loadTokens() {
        SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
        try {
            ResultSet rs = stmt.executeQuery("SELECT peer, tokens FROM " + PEERS_TABLE);
            while (rs.next()) {
                String tokens = rs.getString(2);
                if (tokens != null) {
                    List<String> list = Arrays.asList(tokens.split(","));
                    InetAddress peer = InetAddress.getByName(rs.getString(1));
                    tokenMap.putAll(peer, deserializeTokens(list));
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }
        return tokenMap;
    }

    public static Map<InetAddress, UUID> loadHostIds() {
        Map<InetAddress, UUID> hostIdMap = new HashMap<>();
        try {
            ResultSet rs = stmt.executeQuery("SELECT peer, host_id FROM " + PEERS_TABLE);
            while (rs.next()) {
                String host_id = rs.getString(2);
                if (host_id != null) {
                    InetAddress peer = InetAddress.getByName(rs.getString(1));
                    hostIdMap.put(peer, UUID.fromString(host_id));
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }
        return hostIdMap;
    }

    public static Collection<Token> getSavedTokens() {
        String sql = "SELECT tokens FROM %s WHERE key='%s'";

        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, LOCAL_TABLE, LOCAL_KEY));
            if (rs.next()) {
                String tokens = rs.getString(1);
                if (tokens != null) {
                    List<String> list = Arrays.asList(tokens.split(","));
                    rs.close();
                    return deserializeTokens(list);
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }

        return Collections.<Token> emptyList();
    }

    public static UUID getLocalHostId() {
        String sql = "SELECT host_id FROM %s WHERE key='%s'";

        try {
            ResultSet rs = stmt.executeQuery(String.format(sql, LOCAL_TABLE, LOCAL_KEY));
            if (rs.next()) {
                String host_id = rs.getString(1);
                if (host_id != null) {
                    rs.close();
                    return UUID.fromString(host_id);
                }
            }
            rs.close();
        } catch (Exception e) {
            handleException(e);
        }

        // ID not found, generate a new one, persist, and then return it.
        UUID hostId = UUID.randomUUID();
        logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
        return setLocalHostId(hostId);
    }

    public static UUID setLocalHostId(UUID hostId) {
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

    public static synchronized void updateTokens(InetAddress ep, Collection<Token> tokens) {
        if (ep.equals(Utils.getBroadcastAddress())) {
            removeEndpoint(ep);
            return;
        }

        String sql = "MERGE INTO %s (peer, tokens) KEY(peer) VALUES('%s', '%s')";
        try {
            sql = String.format(sql, PEERS_TABLE, ep.getHostAddress(), StringUtils.join(tokensAsSet(tokens), ','));
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static synchronized void updateTokens(Collection<Token> tokens) {
        assert !tokens.isEmpty() : "removeEndpoint should be used instead";
        String sql = "UPDATE %s SET tokens = '%s' WHERE key = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, LOCAL_TABLE, StringUtils.join(tokensAsSet(tokens), ','), LOCAL_KEY));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    private static Set<String> tokensAsSet(Collection<Token> tokens) {
        TokenFactory factory = StorageService.getPartitioner().getTokenFactory();
        Set<String> s = new HashSet<>(tokens.size());
        for (Token tk : tokens)
            s.add(factory.toString(tk));
        return s;
    }

    // 由调用者确定是否把本地节点的信息存入PEERS表
    public static synchronized void updatePeerInfo(InetAddress ep, String columnName, Object value) {
        String sql = "MERGE INTO %s (peer, %s) KEY(peer) VALUES('%s', '%s')";
        try {
            // InetAddress.getCanonicalHostName很慢，别用它
            stmt.executeUpdate(String.format(sql, PEERS_TABLE, columnName, ep.getHostAddress(), value));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    public static synchronized void removeEndpoint(InetAddress ep) {
        String sql = "DELETE FROM %s WHERE peer = '%s'";
        try {
            stmt.executeUpdate(String.format(sql, PEERS_TABLE, ep.getHostAddress()));
        } catch (SQLException e) {
            handleException(e);
        }
    }

    /**
     * Convenience method to update the list of tokens in the local system keyspace.
     *
     * @param addTokens tokens to add
     * @param rmTokens tokens to remove
     * @return the collection of persisted tokens
     */
    public static synchronized Collection<Token> updateLocalTokens(Collection<Token> addTokens,
            Collection<Token> rmTokens) {
        Collection<Token> tokens = getSavedTokens();
        tokens.removeAll(rmTokens);
        tokens.addAll(addTokens);
        updateTokens(tokens);
        return tokens;
    }
}
