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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.auth.AllowAllInternodeAuthenticator;
import org.lealone.aose.auth.IInternodeAuthenticator;
import org.lealone.aose.config.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.aose.config.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.aose.locator.AbstractReplicationStrategy;
import org.lealone.aose.locator.DynamicEndpointSnitch;
import org.lealone.aose.locator.EndpointSnitchInfo;
import org.lealone.aose.locator.IEndpointSnitch;
import org.lealone.aose.locator.SeedProvider;
import org.lealone.aose.locator.SimpleStrategy;
import org.lealone.aose.net.MessagingService;
import org.lealone.aose.server.StorageServer;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigurationException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ConfigDescriptor {

    private static Config conf;
    private static IInternodeAuthenticator internodeAuthenticator;

    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress broadcastRpcAddress;

    private static IEndpointSnitch snitch;
    private static String localDC;
    private static Comparator<InetAddress> localComparator;
    private static SeedProvider seedProvider;
    private static AbstractReplicationStrategy defaultReplicationStrategy;

    public static void applyConfig(Config config) throws ConfigurationException {
        conf = config;

        if (conf.internode_authenticator != null)
            internodeAuthenticator = Utils.construct(conf.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        internodeAuthenticator.validateConfiguration();

        // phi convict threshold for FailureDetector
        if (conf.phi_convict_threshold < 5 || conf.phi_convict_threshold > 16) {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16");
        }

        applyAddressConfig(config);
        createEndpointSnitch();

        localDC = snitch.getDatacenter(Utils.getBroadcastAddress());
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

        createSeedProvider();
        initDefaultReplicationStrategy();
    }

    private static void applyAddressConfig(Config conf) throws ConfigurationException {
        // Local IP, hostname or interface to bind services to
        if (conf.listen_address != null && conf.listen_interface != null) {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both");
        } else if (conf.listen_address != null) {
            try {
                listenAddress = InetAddress.getByName(conf.listen_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown listen_address '" + conf.listen_address + "'");
            }

            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException(
                        "listen_address cannot be a wildcard address (" + conf.listen_address + ")!");
        } else if (conf.listen_interface != null) {
            listenAddress = getNetworkInterfaceAddress(conf.listen_interface, "listen_interface",
                    conf.listen_interface_prefer_ipv6);
        } else {
            listenAddress = Utils.getLocalAddress();
            conf.listen_address = listenAddress.getHostAddress();
        }

        // Gossip Address to broadcast
        if (conf.broadcast_address != null) {
            try {
                broadcastAddress = InetAddress.getByName(conf.broadcast_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown broadcast_address '" + conf.broadcast_address + "'");
            }

            if (broadcastAddress.isAnyLocalAddress())
                throw new ConfigurationException(
                        "broadcast_address cannot be a wildcard address (" + conf.broadcast_address + ")!");
        }

        // RPC address to broadcast
        if (conf.broadcast_rpc_address != null) {
            try {
                broadcastRpcAddress = InetAddress.getByName(conf.broadcast_rpc_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown broadcast_rpc_address '" + conf.broadcast_rpc_address + "'");
            }

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException(
                        "broadcast_rpc_address cannot be a wildcard address (" + conf.broadcast_rpc_address + ")!");
        } else {
            broadcastRpcAddress = Utils.getLocalAddress();

            if (broadcastRpcAddress.isAnyLocalAddress())
                throw new ConfigurationException(
                        "If rpc_address is set to a wildcard address (" + broadcastRpcAddress + "), then "
                                + "you must set broadcast_rpc_address to a value other than " + broadcastRpcAddress);
        }
    }

    private static InetAddress getNetworkInterfaceAddress(String intf, String configName, boolean preferIPv6)
            throws ConfigurationException {
        try {
            NetworkInterface ni = NetworkInterface.getByName(intf);
            if (ni == null)
                throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" could not be found");
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            if (!addrs.hasMoreElements())
                throw new ConfigurationException(
                        "Configured " + configName + " \"" + intf + "\" was found, but had no addresses");
            // Try to return the first address of the preferred type, otherwise return the first address
            InetAddress retval = null;
            while (addrs.hasMoreElements()) {
                InetAddress temp = addrs.nextElement();
                if (preferIPv6 && temp instanceof Inet6Address)
                    return temp;
                if (!preferIPv6 && temp instanceof Inet4Address)
                    return temp;
                if (retval == null)
                    retval = temp;
            }
            return retval;
        } catch (SocketException e) {
            throw new ConfigurationException("Configured " + configName + " \"" + intf + "\" caused an exception", e);
        }
    }

    private static void createEndpointSnitch() throws ConfigurationException {
        // end point snitch
        if (conf.endpoint_snitch == null) {
            throw new ConfigurationException("Missing endpoint_snitch directive");
        }

        String className = conf.endpoint_snitch;
        if (!className.contains("."))
            className = IEndpointSnitch.class.getPackage().getName() + "." + className;
        snitch = Utils.construct(className, "snitch");
        if (conf.dynamic_snitch)
            snitch = new DynamicEndpointSnitch(snitch);

        EndpointSnitchInfo.create();
    }

    private static void createSeedProvider() throws ConfigurationException {
        if (conf.seed_provider == null) {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.");
        }

        String className = conf.seed_provider.class_name;
        if (!className.contains("."))
            className = SeedProvider.class.getPackage().getName() + "." + className;
        try {
            Class<?> seedProviderClass = Class.forName(className);
            seedProvider = (SeedProvider) seedProviderClass.getConstructor(Map.class)
                    .newInstance(conf.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e) {
            throw new ConfigurationException(
                    e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
        if (seedProvider.getSeeds().isEmpty())
            throw new ConfigurationException("The seed provider lists no seeds.");
    }

    private static void initDefaultReplicationStrategy() throws ConfigurationException {
        if (conf.replication_strategy == null)
            defaultReplicationStrategy = new SimpleStrategy("system", StorageServer.instance.getTopologyMetaData(),
                    getEndpointSnitch(), ImmutableMap.of("replication_factor", "1"));
        else
            defaultReplicationStrategy = AbstractReplicationStrategy.createReplicationStrategy("system",
                    AbstractReplicationStrategy.getClass(conf.replication_strategy.class_name),
                    StorageServer.instance.getTopologyMetaData(), getEndpointSnitch(),
                    conf.replication_strategy.parameters);
    }

    public static AbstractReplicationStrategy getDefaultReplicationStrategy() {
        return defaultReplicationStrategy;
    }

    public static IEndpointSnitch getEndpointSnitch() {
        return snitch;
    }

    public static String getClusterName() {
        return conf.cluster_name;
    }

    public static int getStoragePort() {
        return Integer.parseInt(Config.getProperty("storage.port", conf.storage_port.toString()));
    }

    public static int getSSLStoragePort() {
        return Integer.parseInt(Config.getProperty("ssl.storage.port", conf.ssl_storage_port.toString()));
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

    public static List<InetAddress> getSeedList() {
        return seedProvider.getSeeds();
    }

    public static InetAddress getListenAddress() {
        return listenAddress;
    }

    public static InetAddress getBroadcastAddress() {
        return broadcastAddress;
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

    public static IInternodeAuthenticator getInternodeAuthenticator() {
        return internodeAuthenticator;
    }

    public static Integer getInternodeSendBufferSize() {
        return conf.internode_send_buff_size_in_bytes;
    }

    public static Integer getInternodeRecvBufferSize() {
        return conf.internode_recv_buff_size_in_bytes;
    }

    public static boolean isAutoBootstrap() {
        return Boolean.parseBoolean(Config.getProperty("auto.bootstrap", conf.auto_bootstrap.toString()));
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
        // currently we just check if it's a 64bit arch,
        // but any we only really care if the address space is large
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

    public static int getStreamThroughputOutboundMegabitsPerSec() {
        return conf.stream_throughput_outbound_megabits_per_sec;
    }

    public static void setStreamThroughputOutboundMegabitsPerSec(int value) {
        conf.stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getInterDCStreamThroughputOutboundMegabitsPerSec() {
        return conf.inter_dc_stream_throughput_outbound_megabits_per_sec;
    }

    public static void setInterDCStreamThroughputOutboundMegabitsPerSec(int value) {
        conf.inter_dc_stream_throughput_outbound_megabits_per_sec = value;
    }

    public static int getStreamingSocketTimeout() {
        return conf.streaming_socket_timeout_in_ms;
    }

    public static int getMaxStreamingRetries() {
        return conf.max_streaming_retries;
    }

    public static Integer getHostId() {
        return conf.host_id;
    }
}
