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
package org.lealone.p2p.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;

public class Config {

    public String base_dir;

    public String listen_address;
    public String listen_interface;
    public Boolean listen_interface_prefer_ipv6 = false;

    public List<PluggableEngineDef> storage_engines;
    public List<PluggableEngineDef> transaction_engines;
    public List<PluggableEngineDef> sql_engines;
    public List<PluggableEngineDef> protocol_server_engines;

    public ServerEncryptionOptions server_encryption_options;
    public ClientEncryptionOptions client_encryption_options;

    public ClusterConfig cluster_config = new ClusterConfig();

    public static String getProperty(String key) {
        return getProperty(key, null);
    }

    public static String getProperty(String key, String def) {
        return System.getProperty(Constants.PROJECT_NAME_PREFIX + key, def);
    }

    public static void setProperty(String key, String value) {
        System.setProperty(Constants.PROJECT_NAME_PREFIX + key, value);
    }

    public static class ClusterConfig {

        public String cluster_name = "Test Cluster";

        public String endpoint_snitch;
        public Boolean dynamic_snitch = true;
        public Integer dynamic_snitch_update_interval_in_ms = 100;
        public Integer dynamic_snitch_reset_interval_in_ms = 600000;
        public Double dynamic_snitch_badness_threshold = 0.1;

        public volatile Long request_timeout_in_ms = 10000L;
        public volatile Double phi_convict_threshold = 8.0;
        public boolean cross_node_timeout = false;

        public String internode_authenticator;

        public SeedProviderDef seed_provider;
        public ReplicationStrategyDef replication_strategy;
        public EndpointAssignmentStrategyDef endpoint_assignment_strategy;

    }

    public static abstract class MapPropertyTypeDef {
        public String name;
        public Map<String, String> parameters = new HashMap<>();

        public MapPropertyTypeDef() {
        }

        public Map<String, String> getParameters() {
            return parameters;
        }
    }

    public static class SeedProviderDef extends MapPropertyTypeDef {
    }

    public static class ReplicationStrategyDef extends MapPropertyTypeDef {
    }

    public static class EndpointAssignmentStrategyDef extends MapPropertyTypeDef {
    }

    public static class PluggableEngineDef extends MapPropertyTypeDef {
        public Boolean enabled = true;
    }
}
