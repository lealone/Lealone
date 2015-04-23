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
package org.lealone.cluster.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

import org.lealone.cluster.config.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.cluster.config.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.google.common.collect.Sets;

/**
 * A class that contains configuration properties for the lealone node it runs within.
 */
public class Config {
    public String cluster_name = "Test Cluster";
    public String partitioner;

    public Boolean auto_bootstrap = true;
    public volatile boolean hinted_handoff_enabled_global = true;
    public String hinted_handoff_enabled;
    public Set<String> hinted_handoff_enabled_by_dc = Sets.newConcurrentHashSet();
    public volatile Integer max_hint_window_in_ms = 3600 * 1000; // one hour

    public SeedProviderDef seed_provider;

    public Integer num_tokens = 1;

    public volatile Long request_timeout_in_ms = 10000L;

    public volatile Long read_request_timeout_in_ms = 5000L;

    public volatile Long range_request_timeout_in_ms = 10000L;

    public volatile Long write_request_timeout_in_ms = 2000L;

    public volatile Long truncate_request_timeout_in_ms = 60000L;

    public boolean cross_node_timeout = false;

    public volatile Double phi_convict_threshold = 8.0;

    public Integer concurrent_reads = 32;
    public Integer concurrent_writes = 32;

    public Integer storage_port = 6210;
    public Integer ssl_storage_port = 6211;
    public String listen_address;
    public String listen_interface;
    public String broadcast_address;
    public String internode_authenticator;

    public String broadcast_rpc_address;

    public Integer internode_send_buff_size_in_bytes;
    public Integer internode_recv_buff_size_in_bytes;

    public String endpoint_snitch;
    public Boolean dynamic_snitch = true;
    public Integer dynamic_snitch_update_interval_in_ms = 100;
    public Integer dynamic_snitch_reset_interval_in_ms = 600000;
    public Double dynamic_snitch_badness_threshold = 0.1;

    public ServerEncryptionOptions server_encryption_options = new ServerEncryptionOptions();
    public ClientEncryptionOptions client_encryption_options = new ClientEncryptionOptions();

    public InternodeCompression internode_compression = InternodeCompression.none;

    public int hinted_handoff_throttle_in_kb = 1024;
    public int batchlog_replay_throttle_in_kb = 1024;
    public int max_hints_delivery_threads = 1;

    public Long key_cache_size_in_mb = null;
    public volatile int key_cache_save_period = 14400;
    public volatile int key_cache_keys_to_save = Integer.MAX_VALUE;

    public long row_cache_size_in_mb = 0;
    public volatile int row_cache_save_period = 0;
    public volatile int row_cache_keys_to_save = Integer.MAX_VALUE;

    public boolean inter_dc_tcp_nodelay = true;

    private static boolean outboundBindAny = false;

    private static final CsvPreference STANDARD_SURROUNDING_SPACES_NEED_QUOTES = new CsvPreference.Builder(
            CsvPreference.STANDARD_PREFERENCE).surroundingSpacesNeedQuotes(true).build();

    // TTL for different types of trace events.
    public Integer tracetype_query_ttl = 60 * 60 * 24;
    public Integer tracetype_repair_ttl = 60 * 60 * 24 * 7;

    //TCP Server
    public String base_dir;
    public int tcp_port = 0;
    public boolean tcp_daemon = false;
    public boolean tcp_allow_others = true;

    public RunMode run_mode = RunMode.cluster;

    public boolean isClusterMode() {
        return run_mode == RunMode.cluster;
    }

    public static boolean getOutboundBindAny() {
        return outboundBindAny;
    }

    public static void setOutboundBindAny(boolean value) {
        outboundBindAny = value;
    }

    public void configHintedHandoff() throws ConfigurationException {
        if (hinted_handoff_enabled != null && !hinted_handoff_enabled.isEmpty()) {
            if (hinted_handoff_enabled.equalsIgnoreCase("true")) {
                hinted_handoff_enabled_global = true;
            } else if (hinted_handoff_enabled.equalsIgnoreCase("false")) {
                hinted_handoff_enabled_global = false;
            } else {
                try {
                    hinted_handoff_enabled_by_dc.addAll(parseHintedHandoffEnabledDCs(hinted_handoff_enabled));
                } catch (IOException e) {
                    throw new ConfigurationException("Invalid hinted_handoff_enabled parameter "
                            + hinted_handoff_enabled, e);
                }
            }
        }
    }

    public static List<String> parseHintedHandoffEnabledDCs(final String dcNames) throws IOException {
        try (final CsvListReader csvListReader = new CsvListReader(new StringReader(dcNames),
                STANDARD_SURROUNDING_SPACES_NEED_QUOTES)) {
            return csvListReader.read();
        }
    }

    public static enum InternodeCompression {
        all,
        none,
        dc
    }

    public static enum RunMode {
        //embedded,
        client_server,
        //standalone,
        cluster,
    }
}
