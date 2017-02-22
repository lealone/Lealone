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
package org.lealone.aose.config;

import java.util.List;

import org.lealone.aose.config.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.aose.config.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;

public class Config {

    public String base_dir;

    public List<PluggableEngineDef> storage_engines;
    public List<PluggableEngineDef> transaction_engines;
    public List<PluggableEngineDef> sql_engines;
    public List<PluggableEngineDef> protocol_server_engines;

    public ServerEncryptionOptions server_encryption_options = new ServerEncryptionOptions();
    public ClientEncryptionOptions client_encryption_options = new ClientEncryptionOptions();

    public String cluster_name = "Test Cluster";

    public Integer host_id;

    public Boolean auto_bootstrap = true;

    public SeedProviderDef seed_provider;
    public ReplicationStrategyDef replication_strategy;

    public volatile Long request_timeout_in_ms = 10000L;

    public boolean cross_node_timeout = false;

    public volatile Double phi_convict_threshold = 8.0;

    public Integer storage_port = 6210;
    public Integer ssl_storage_port = 6211;

    public Integer listen_port;
    public String listen_address;
    public String listen_interface;
    public Boolean listen_interface_prefer_ipv6 = false;
    public String broadcast_address;
    public String broadcast_rpc_address;

    public String internode_authenticator;

    public Integer internode_send_buff_size_in_bytes;
    public Integer internode_recv_buff_size_in_bytes;

    public String endpoint_snitch;
    public Boolean dynamic_snitch = true;
    public Integer dynamic_snitch_update_interval_in_ms = 100;
    public Integer dynamic_snitch_reset_interval_in_ms = 600000;
    public Double dynamic_snitch_badness_threshold = 0.1;

    public volatile Integer stream_throughput_outbound_megabits_per_sec = 200;
    public volatile Integer inter_dc_stream_throughput_outbound_megabits_per_sec = 0;
    public Integer streaming_socket_timeout_in_ms = 0;
    public Integer max_streaming_retries = 3;

    public InternodeCompression internode_compression = InternodeCompression.none;

    public boolean inter_dc_tcp_nodelay = true;

    private static boolean outboundBindAny = false;

    public static boolean getOutboundBindAny() {
        return outboundBindAny;
    }

    public static void setOutboundBindAny(boolean value) {
        outboundBindAny = value;
    }

    public static enum InternodeCompression {
        all,
        none,
        dc
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

}
