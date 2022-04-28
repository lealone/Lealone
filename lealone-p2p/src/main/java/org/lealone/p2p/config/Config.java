/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.config;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.Constants;

public class Config {

    public String base_dir = "." + File.separator + Constants.PROJECT_NAME + "_data";

    public String listen_address = "127.0.0.1";
    public String listen_interface;
    public Boolean listen_interface_prefer_ipv6 = false;

    public List<PluggableEngineDef> storage_engines;
    public List<PluggableEngineDef> transaction_engines;
    public List<PluggableEngineDef> sql_engines;
    public List<PluggableEngineDef> protocol_server_engines;

    public ServerEncryptionOptions server_encryption_options;
    public ClientEncryptionOptions client_encryption_options;

    public ClusterConfig cluster_config = new ClusterConfig();

    public Config() {
    }

    public Config(boolean isDefault) {
        if (!isDefault)
            return;
        storage_engines = new ArrayList<>(1);
        PluggableEngineDef se = new PluggableEngineDef();
        se.name = Constants.DEFAULT_STORAGE_ENGINE_NAME;
        se.enabled = true;
        storage_engines.add(se);

        transaction_engines = new ArrayList<>(1);
        PluggableEngineDef te = new PluggableEngineDef();
        te.name = Constants.DEFAULT_TRANSACTION_ENGINE_NAME;
        te.enabled = true;
        te.parameters.put("redo_log_dir", "redo_log");
        transaction_engines.add(te);

        sql_engines = new ArrayList<>(1);
        PluggableEngineDef sql = new PluggableEngineDef();
        sql.name = Constants.DEFAULT_SQL_ENGINE_NAME;
        sql.enabled = true;
        sql_engines.add(sql);

        protocol_server_engines = new ArrayList<>(2);
        PluggableEngineDef tcp = new PluggableEngineDef();
        tcp.name = "TCP";
        tcp.enabled = true;
        tcp.parameters.put("port", Constants.DEFAULT_TCP_PORT + "");
        tcp.parameters.put("trace", "false");
        tcp.parameters.put("allow_others", "true");
        tcp.parameters.put("daemon", "false");
        tcp.parameters.put("ssl", "false");

        PluggableEngineDef p2p = new PluggableEngineDef();
        p2p.name = "P2P";
        p2p.enabled = false;
        p2p.parameters.put("port", Constants.DEFAULT_P2P_PORT + "");
        p2p.parameters.put("ssl", "false");

        protocol_server_engines.add(tcp);
        protocol_server_engines.add(p2p);
    }

    public Map<String, String> getProtocolServerParameters(String name) {
        if (protocol_server_engines != null) {
            for (PluggableEngineDef def : protocol_server_engines) {
                if (name.equalsIgnoreCase(def.name)) {
                    return def.parameters;
                }
            }
        }
        return new HashMap<>(0);
    }

    public static String getProperty(String key) {
        return getProperty(key, null);
    }

    public static String getProperty(String key, String def) {
        return System.getProperty(Constants.PROJECT_NAME_PREFIX + key, def);
    }

    public static void setProperty(String key, String value) {
        System.setProperty(Constants.PROJECT_NAME_PREFIX + key, value);
    }

    public static Config getDefaultConfig() {
        return new Config(true);
    }

    public static Config mergeDefaultConfig(Config c) { // c是custom config
        Config d = getDefaultConfig();
        if (c == null)
            return d;
        c.storage_engines = mergeEngines(c.storage_engines, d.storage_engines);
        c.transaction_engines = mergeEngines(c.transaction_engines, d.transaction_engines);
        c.sql_engines = mergeEngines(c.sql_engines, d.sql_engines);
        c.protocol_server_engines = mergeEngines(c.protocol_server_engines, d.protocol_server_engines);
        if (c.cluster_config == null) {
            c.cluster_config = d.cluster_config;
        } else {
            c.cluster_config.merge(d.cluster_config);
        }
        return c;
    }

    private static List<PluggableEngineDef> mergeEngines(List<PluggableEngineDef> newList,
            List<PluggableEngineDef> defaultList) {
        if (defaultList == null)
            return newList;
        if (newList == null)
            return defaultList;
        CaseInsensitiveMap<PluggableEngineDef> map = new CaseInsensitiveMap<>();
        for (PluggableEngineDef e : defaultList) {
            map.put(e.name, e);
        }
        for (PluggableEngineDef e : newList) {
            PluggableEngineDef defaultE = map.get(e.name);
            if (defaultE == null) {
                map.put(e.name, e);
            } else {
                defaultE.enabled = e.enabled;
                defaultE.parameters.putAll(e.parameters);
            }
        }
        return new ArrayList<>(map.values());
    }

    private static <T extends MapPropertyTypeDef> T mergeMap(T defaultMap, T newMap) {
        if (defaultMap == null)
            return newMap;
        if (newMap == null)
            return defaultMap;
        defaultMap.parameters.putAll(defaultMap.parameters);
        return defaultMap;
    }

    public static class ClusterConfig {

        public String cluster_name = "Test Cluster";

        public String node_snitch = "SimpleSnitch";
        public Boolean dynamic_snitch = true;
        public Integer dynamic_snitch_update_interval_in_ms = 100;
        public Integer dynamic_snitch_reset_interval_in_ms = 600000;
        public Double dynamic_snitch_badness_threshold = 0.1;

        public Integer request_timeout_in_ms = 10000; // 默认10秒
        public Double phi_convict_threshold = 8.0;
        public boolean cross_node_timeout = false;

        public String internode_authenticator;

        public SeedProviderDef seed_provider;
        public ReplicationStrategyDef replication_strategy;
        public NodeAssignmentStrategyDef node_assignment_strategy;

        public ClusterConfig() {
            seed_provider = new SeedProviderDef();
            seed_provider.name = "SimpleSeedProvider";
            seed_provider.parameters.put("seeds", "127.0.0.1");

            replication_strategy = new ReplicationStrategyDef();
            replication_strategy.name = "SimpleStrategy";
            replication_strategy.parameters.put("replication_factor", "3");
        }

        public void merge(ClusterConfig c) {
            seed_provider = mergeMap(seed_provider, c.seed_provider);
            replication_strategy = mergeMap(replication_strategy, c.replication_strategy);
            node_assignment_strategy = mergeMap(node_assignment_strategy, c.node_assignment_strategy);
        }
    }

    public static abstract class MapPropertyTypeDef {
        public String name;
        public Map<String, String> parameters = new HashMap<>();

        public MapPropertyTypeDef() {
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public void setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
        }
    }

    public static class SeedProviderDef extends MapPropertyTypeDef {
    }

    public static class ReplicationStrategyDef extends MapPropertyTypeDef {
    }

    public static class NodeAssignmentStrategyDef extends MapPropertyTypeDef {
    }

    public static class PluggableEngineDef extends MapPropertyTypeDef {
        public Boolean enabled = true;
    }
}
