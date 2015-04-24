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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.lealone.cluster.auth.AllowAllInternodeAuthenticator;
import org.lealone.cluster.auth.IInternodeAuthenticator;
import org.lealone.cluster.config.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.cluster.config.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.cluster.db.SystemKeyspace;
import org.lealone.cluster.dht.IPartitioner;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.DynamicEndpointSnitch;
import org.lealone.cluster.locator.EndpointSnitchInfo;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.locator.SeedProvider;
import org.lealone.cluster.locator.SimpleStrategy;
import org.lealone.cluster.net.MessagingService;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.utils.FBUtilities;
import org.lealone.cluster.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class DatabaseDescriptor {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDescriptor.class);

    /**
     * Tokens are serialized in a Gossip VersionedValue String.  VV are restricted to 64KB
     * when we send them over the wire, which works out to about 1700 tokens.
     */
    private static final int MAX_NUM_TOKENS = 1536;

    private static IEndpointSnitch snitch;
    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress broadcastRpcAddress;
    private static SeedProvider seedProvider;
    private static IInternodeAuthenticator internodeAuthenticator;

    private static IPartitioner partitioner;
    private static String paritionerName;
    private static AbstractReplicationStrategy defaultReplicationStrategy;

    private static Config conf;

    private static String localDC;
    private static Comparator<InetAddress> localComparator;

    public static Config loadConfig() throws ConfigurationException {
        if (conf != null)
            return conf;

        String loaderClass = System.getProperty("lealone.config.loader");
        ConfigurationLoader loader = loaderClass == null ? new YamlConfigurationLoader() : FBUtilities
                .<ConfigurationLoader> construct(loaderClass, "configuration loading");
        Config conf = loader.loadConfig();

        try {
            applyConfig(conf);
        } catch (Exception e) {
            logger.error("", e);
            JVMStabilityInspector.inspectThrowable(e);
            throw new ExceptionInInitializerError(e.getMessage()
                    + "\nFatal configuration error; unable to start. See log for stacktrace.");
        }
        return conf;
    }

    private static void applyConfig(Config config) throws ConfigurationException {
        conf = config;

        if (conf.internode_authenticator != null)
            internodeAuthenticator = FBUtilities.construct(conf.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        internodeAuthenticator.validateConfiguration();

        if (conf.partitioner == null) {
            throw new ConfigurationException("Missing directive: partitioner");
        }
        try {
            partitioner = FBUtilities.newPartitioner(System.getProperty("lealone.partitioner", conf.partitioner));
        } catch (Exception e) {
            throw new ConfigurationException("Invalid partitioner class " + conf.partitioner);
        }
        paritionerName = partitioner.getClass().getCanonicalName();

        /* phi convict threshold for FailureDetector */
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16) {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16");
        }

        /* Local IP, hostname or interface to bind services to */
        if (conf.listen_address != null && conf.listen_interface != null) {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both");
        } else if (conf.listen_address != null) {
            try {
                listenAddress = InetAddress.getByName(conf.listen_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown listen_address '" + conf.listen_address + "'");
            }

            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException("listen_address cannot be a wildcard address (" + conf.listen_address
                        + ")!");
        } else if (conf.listen_interface != null) {
            try {
                Enumeration<InetAddress> addrs = NetworkInterface.getByName(conf.listen_interface).getInetAddresses();
                listenAddress = addrs.nextElement();
                if (addrs.hasMoreElements())
                    throw new ConfigurationException("Interface " + conf.listen_interface
                            + " can't have more than one address");
            } catch (SocketException e) {
                throw new ConfigurationException("Unknown network interface in listen_interface "
                        + conf.listen_interface);
            }

        }

        /* Gossip Address to broadcast */
        if (conf.broadcast_address != null) {
            try {
                broadcastAddress = InetAddress.getByName(conf.broadcast_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown broadcast_address '" + conf.broadcast_address + "'");
            }

            if (broadcastAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_address cannot be a wildcard address ("
                        + conf.broadcast_address + ")!");
        }

        /* RPC address to broadcast */
        if (conf.broadcast_rpc_address != null) {
            try {
                broadcastRpcAddress = InetAddress.getByName(conf.broadcast_rpc_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + conf.broadcast_rpc_address + "'");
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("broadcast_rpc_address cannot be a wildcard address ("
                        + conf.broadcast_rpc_address + ")!");
        } else {
            broadcastRpcAddress = FBUtilities.getLocalAddress();

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException("If rpc_address is set to a wildcard address (" + broadcastRpcAddress
                        + "), then " + "you must set broadcast_rpc_address to a value other than "
                        + broadcastRpcAddress);
        }

        /* end point snitch */
        if (conf.endpoint_snitch == null) {
            throw new ConfigurationException("Missing endpoint_snitch directive");
        }
        snitch = createEndpointSnitch(conf.endpoint_snitch);
        EndpointSnitchInfo.create();

        localDC = snitch.getDatacenter(FBUtilities.getBroadcastAddress());
        localComparator = new Comparator<InetAddress>() {
            @Override
            public int compare(InetAddress endpoint1, InetAddress endpoint2) {
                boolean local1 = localDC.equals(snitch.getDatacenter(endpoint1));
                boolean local2 = localDC.equals(snitch.getDatacenter(endpoint2));
                if (local1 && !local2)
                    return -1;
                if (local2 && !local1)
                    return 1;
                return 0;
            }
        };

        if (conf.num_tokens == null)
            conf.num_tokens = 1;
        else if (conf.num_tokens > MAX_NUM_TOKENS)
            throw new ConfigurationException(String.format("A maximum number of %d tokens per node is supported",
                    MAX_NUM_TOKENS));

        if (conf.seed_provider == null) {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.");
        }
        try {
            Class<?> seedProviderClass = Class.forName(conf.seed_provider.class_name);
            seedProvider = (SeedProvider) seedProviderClass.getConstructor(Map.class).newInstance(
                    conf.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e) {
            throw new ConfigurationException(e.getMessage()
                    + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
        if (seedProvider.getSeeds().size() == 0)
            throw new ConfigurationException("The seed provider lists no seeds.");

        initDefaultReplicationStrategy();
    }

    private static void initDefaultReplicationStrategy() throws ConfigurationException {
        if (conf.replication_strategy == null)
            defaultReplicationStrategy = new SimpleStrategy("system", StorageService.instance.getTokenMetadata(),
                    getEndpointSnitch(), ImmutableMap.of("replication_factor", "1"));
        else
            defaultReplicationStrategy = AbstractReplicationStrategy.createReplicationStrategy("system",
                    AbstractReplicationStrategy.getClass(conf.replication_strategy.class_name),
                    StorageService.instance.getTokenMetadata(), getEndpointSnitch(),
                    conf.replication_strategy.parameters);
    }

    private static IEndpointSnitch createEndpointSnitch(String snitchClassName) throws ConfigurationException {
        if (!snitchClassName.contains("."))
            snitchClassName = "org.lealone.cluster.locator." + snitchClassName;
        IEndpointSnitch snitch = FBUtilities.construct(snitchClassName, "snitch");
        return conf.dynamic_snitch ? new DynamicEndpointSnitch(snitch) : snitch;
    }

    public static AbstractReplicationStrategy getDefaultReplicationStrategy() {
        return defaultReplicationStrategy;
    }

    public static IPartitioner getPartitioner() {
        return partitioner;
    }

    public static String getPartitionerName() {
        return paritionerName;
    }

    public static IEndpointSnitch getEndpointSnitch() {
        return snitch;
    }

    public static Collection<String> tokensFromString(String tokenString) {
        List<String> tokens = new ArrayList<String>();
        if (tokenString != null)
            for (String token : tokenString.split(","))
                tokens.add(token.replaceAll("^\\s+", "").replaceAll("\\s+$", ""));
        return tokens;
    }

    public static Integer getNumTokens() {
        return conf.num_tokens;
    }

    public static InetAddress getReplaceAddress() {
        try {
            if (System.getProperty("lealone.replace_address", null) != null)
                return InetAddress.getByName(System.getProperty("lealone.replace_address", null));
            else if (System.getProperty("lealone.replace_address_first_boot", null) != null)
                return InetAddress.getByName(System.getProperty("lealone.replace_address_first_boot", null));
            return null;
        } catch (UnknownHostException e) {
            return null;
        }
    }

    public static Collection<String> getReplaceTokens() {
        return tokensFromString(System.getProperty("lealone.replace_token", null));
    }

    public static UUID getReplaceNode() {
        try {
            return UUID.fromString(System.getProperty("lealone.replace_node", null));
        } catch (NullPointerException e) {
            return null;
        }
    }

    public static boolean isReplacing() {
        if (System.getProperty("lealone.replace_address_first_boot", null) != null
                && SystemKeyspace.bootstrapComplete()) {
            logger.info("Replace address on first boot requested; this node is already bootstrapped");
            return false;
        }
        return getReplaceAddress() != null;
    }

    public static String getClusterName() {
        return conf.cluster_name;
    }

    public static int getStoragePort() {
        return Integer.parseInt(System.getProperty("lealone.storage_port", conf.storage_port.toString()));
    }

    public static int getSSLStoragePort() {
        return Integer.parseInt(System.getProperty("lealone.ssl_storage_port", conf.ssl_storage_port.toString()));
    }

    public static long getRpcTimeout() {
        return conf.request_timeout_in_ms;
    }

    public static boolean hasCrossNodeTimeout() {
        return conf.cross_node_timeout;
    }

    // not part of the Verb enum so we can change timeouts easily via JMX
    public static long getTimeout(MessagingService.Verb verb) {
        return getRpcTimeout();
    }

    public static double getPhiConvictThreshold() {
        return conf.phi_convict_threshold;
    }

    public static void setPhiConvictThreshold(double phiConvictThreshold) {
        conf.phi_convict_threshold = phiConvictThreshold;
    }

    public static Set<InetAddress> getSeeds() {
        return ImmutableSet.<InetAddress> builder().addAll(seedProvider.getSeeds()).build();
    }

    public static InetAddress getListenAddress() {
        return listenAddress;
    }

    public static InetAddress getBroadcastAddress() {
        return broadcastAddress;
    }

    public static IInternodeAuthenticator getInternodeAuthenticator() {
        return internodeAuthenticator;
    }

    public static void setBroadcastAddress(InetAddress broadcastAdd) {
        broadcastAddress = broadcastAdd;
    }

    public static void setBroadcastRpcAddress(InetAddress broadcastRPCAddr) {
        broadcastRpcAddress = broadcastRPCAddr;
    }

    public static InetAddress getBroadcastRpcAddress() {
        return broadcastRpcAddress;
    }

    public static Integer getInternodeSendBufferSize() {
        return conf.internode_send_buff_size_in_bytes;
    }

    public static Integer getInternodeRecvBufferSize() {
        return conf.internode_recv_buff_size_in_bytes;
    }

    public static boolean isAutoBootstrap() {
        return Boolean.parseBoolean(System.getProperty("lealone.auto_bootstrap", conf.auto_bootstrap.toString()));
    }

    public static int getDynamicUpdateInterval() {
        return conf.dynamic_snitch_update_interval_in_ms;
    }

    public static void setDynamicUpdateInterval(Integer dynamicUpdateInterval) {
        conf.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
    }

    public static int getDynamicResetInterval() {
        return conf.dynamic_snitch_reset_interval_in_ms;
    }

    public static void setDynamicResetInterval(Integer dynamicResetInterval) {
        conf.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
    }

    public static double getDynamicBadnessThreshold() {
        return conf.dynamic_snitch_badness_threshold;
    }

    public static void setDynamicBadnessThreshold(Double dynamicBadnessThreshold) {
        conf.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public static ServerEncryptionOptions getServerEncryptionOptions() {
        return conf.server_encryption_options;
    }

    public static ClientEncryptionOptions getClientEncryptionOptions() {
        return conf.client_encryption_options;
    }

    public static String getLocalDataCenter() {
        return localDC;
    }

    public static Comparator<InetAddress> getLocalComparator() {
        return localComparator;
    }

    public static Config.InternodeCompression internodeCompression() {
        return conf.internode_compression;
    }

    public static boolean getInterDCTcpNoDelay() {
        return conf.inter_dc_tcp_nodelay;
    }

    public static boolean hasLargeAddressSpace() {
        // currently we just check if it's a 64bit arch, but any we only really care if the address space is large
        String datamodel = System.getProperty("sun.arch.data.model");
        if (datamodel != null) {
            switch (datamodel) {
            case "64":
                return true;
            case "32":
                return false;
            }
        }
        String arch = System.getProperty("os.arch");
        return arch.contains("64") || arch.contains("sparcv9");
    }
}
