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
import org.lealone.aose.config.Config.ClusterConfig;
import org.lealone.aose.locator.AbstractReplicationStrategy;
import org.lealone.aose.locator.DynamicEndpointSnitch;
import org.lealone.aose.locator.EndpointSnitchInfo;
import org.lealone.aose.locator.IEndpointSnitch;
import org.lealone.aose.locator.SeedProvider;
import org.lealone.aose.locator.SimpleStrategy;
import org.lealone.aose.net.MessagingService;
import org.lealone.aose.server.P2pServer;
import org.lealone.aose.server.P2pServerEngine;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;
import org.lealone.net.NetEndpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class ConfigDescriptor {

    private static Config config;
    private static NetEndpoint localEndpoint;
    private static IEndpointSnitch snitch;
    private static String localDC;
    private static Comparator<NetEndpoint> localComparator;
    private static SeedProvider seedProvider;
    private static IInternodeAuthenticator internodeAuthenticator;
    private static AbstractReplicationStrategy defaultReplicationStrategy;

    public static void applyConfig(Config config) throws ConfigurationException {
        ConfigDescriptor.config = config;
        // phi convict threshold for FailureDetector
        if (config.cluster_config.phi_convict_threshold < 5 || config.cluster_config.phi_convict_threshold > 16) {
            throw new ConfigurationException("phi_convict_threshold must be between 5 and 16");
        }

        localEndpoint = createLocalEndpoint(config);
        snitch = createEndpointSnitch(config.cluster_config);

        localDC = snitch.getDatacenter(localEndpoint);
        localComparator = new Comparator<NetEndpoint>() {
            @Override
            public int compare(NetEndpoint endpoint1, NetEndpoint endpoint2) {
                boolean local1 = localDC.equals(snitch.getDatacenter(endpoint1));
                boolean local2 = localDC.equals(snitch.getDatacenter(endpoint2));
                if (local1 && !local2)
                    return -1;
                if (local2 && !local1)
                    return 1;
                return 0;
            }
        };

        seedProvider = createSeedProvider(config.cluster_config);
        internodeAuthenticator = createInternodeAuthenticator(config.cluster_config);
        defaultReplicationStrategy = createDefaultReplicationStrategy(config.cluster_config);
    }

    private static NetEndpoint createLocalEndpoint(Config config) throws ConfigurationException {
        InetAddress listenAddress = null;
        // Local IP, hostname or interface to bind services to
        if (config.listen_address != null && config.listen_interface != null) {
            throw new ConfigurationException("Set listen_address OR listen_interface, not both");
        } else if (config.listen_address != null) {
            try {
                listenAddress = InetAddress.getByName(config.listen_address);
            } catch (UnknownHostException e) {
                throw new ConfigurationException("Unknown listen_address '" + config.listen_address + "'");
            }
            if (listenAddress.isAnyLocalAddress())
                throw new ConfigurationException(
                        "listen_address cannot be a wildcard address (" + config.listen_address + ")!");
        } else if (config.listen_interface != null) {
            listenAddress = getNetworkInterfaceAddress(config.listen_interface, "listen_interface",
                    config.listen_interface_prefer_ipv6);
        }

        if (listenAddress == null) {
            try {
                listenAddress = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        config.listen_address = listenAddress.getHostAddress();
        String host = config.listen_address;
        int port = Constants.DEFAULT_P2P_PORT;
        if (config.protocol_server_engines != null) {
            for (PluggableEngineDef def : config.protocol_server_engines) {
                if (def.enabled && def.name.equalsIgnoreCase(P2pServerEngine.NAME)) {
                    Map<String, String> parameters = def.getParameters();
                    if (parameters.containsKey("host"))
                        host = parameters.get("host");
                    if (parameters.containsKey("port"))
                        port = Integer.parseInt(parameters.get("port"));
                    break;
                }
            }
        }
        return new NetEndpoint(host, port);
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

    private static IEndpointSnitch createEndpointSnitch(ClusterConfig config) throws ConfigurationException {
        // end point snitch
        if (config.endpoint_snitch == null) {
            throw new ConfigurationException("Missing endpoint_snitch directive");
        }

        String className = config.endpoint_snitch;
        if (!className.contains("."))
            className = IEndpointSnitch.class.getPackage().getName() + "." + className;
        IEndpointSnitch snitch = Utils.construct(className, "snitch");
        if (config.dynamic_snitch)
            snitch = new DynamicEndpointSnitch(snitch);

        EndpointSnitchInfo.create();
        return snitch;
    }

    private static SeedProvider createSeedProvider(ClusterConfig config) throws ConfigurationException {
        if (config.seed_provider == null) {
            throw new ConfigurationException("seeds configuration is missing; a minimum of one seed is required.");
        }
        SeedProvider seedProvider;
        String className = config.seed_provider.class_name;
        if (!className.contains("."))
            className = SeedProvider.class.getPackage().getName() + "." + className;
        try {
            Class<?> seedProviderClass = Class.forName(className);
            seedProvider = (SeedProvider) seedProviderClass.getConstructor(Map.class)
                    .newInstance(config.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e) {
            throw new ConfigurationException(
                    e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
        if (seedProvider.getSeeds().isEmpty())
            throw new ConfigurationException("The seed provider lists no seeds.");
        return seedProvider;
    }

    private static IInternodeAuthenticator createInternodeAuthenticator(ClusterConfig config)
            throws ConfigurationException {
        IInternodeAuthenticator internodeAuthenticator;
        if (config.internode_authenticator != null)
            internodeAuthenticator = Utils.construct(config.internode_authenticator, "internode_authenticator");
        else
            internodeAuthenticator = new AllowAllInternodeAuthenticator();

        internodeAuthenticator.validateConfiguration();
        return internodeAuthenticator;
    }

    private static AbstractReplicationStrategy createDefaultReplicationStrategy(ClusterConfig config)
            throws ConfigurationException {
        AbstractReplicationStrategy defaultReplicationStrategy;
        if (config.replication_strategy == null)
            defaultReplicationStrategy = new SimpleStrategy("system", P2pServer.instance.getTopologyMetaData(),
                    getEndpointSnitch(), ImmutableMap.of("replication_factor", "1"));
        else
            defaultReplicationStrategy = AbstractReplicationStrategy.createReplicationStrategy("system",
                    AbstractReplicationStrategy.getClass(config.replication_strategy.class_name),
                    P2pServer.instance.getTopologyMetaData(), getEndpointSnitch(),
                    config.replication_strategy.parameters);
        return defaultReplicationStrategy;
    }

    public static AbstractReplicationStrategy getDefaultReplicationStrategy() {
        return defaultReplicationStrategy;
    }

    public static IEndpointSnitch getEndpointSnitch() {
        return snitch;
    }

    public static String getClusterName() {
        return config.cluster_config.cluster_name;
    }

    public static long getRpcTimeout() {
        return config.cluster_config.request_timeout_in_ms;
    }

    public static boolean hasCrossNodeTimeout() {
        return config.cluster_config.cross_node_timeout;
    }

    // not part of the Verb enum so we can change timeouts easily via JMX
    public static long getTimeout(MessagingService.Verb verb) {
        return getRpcTimeout();
    }

    public static double getPhiConvictThreshold() {
        return config.cluster_config.phi_convict_threshold;
    }

    public static void setPhiConvictThreshold(double phiConvictThreshold) {
        config.cluster_config.phi_convict_threshold = phiConvictThreshold;
    }

    public static Set<NetEndpoint> getSeeds() {
        return ImmutableSet.<NetEndpoint> builder().addAll(seedProvider.getSeeds()).build();
    }

    public static List<NetEndpoint> getSeedList() {
        return seedProvider.getSeeds();
    }

    public static NetEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    public static IInternodeAuthenticator getInternodeAuthenticator() {
        return internodeAuthenticator;
    }

    public static int getDynamicUpdateInterval() {
        return config.cluster_config.dynamic_snitch_update_interval_in_ms;
    }

    public static void setDynamicUpdateInterval(Integer dynamicUpdateInterval) {
        config.cluster_config.dynamic_snitch_update_interval_in_ms = dynamicUpdateInterval;
    }

    public static int getDynamicResetInterval() {
        return config.cluster_config.dynamic_snitch_reset_interval_in_ms;
    }

    public static void setDynamicResetInterval(Integer dynamicResetInterval) {
        config.cluster_config.dynamic_snitch_reset_interval_in_ms = dynamicResetInterval;
    }

    public static double getDynamicBadnessThreshold() {
        return config.cluster_config.dynamic_snitch_badness_threshold;
    }

    public static void setDynamicBadnessThreshold(Double dynamicBadnessThreshold) {
        config.cluster_config.dynamic_snitch_badness_threshold = dynamicBadnessThreshold;
    }

    public static ServerEncryptionOptions getServerEncryptionOptions() {
        return config.server_encryption_options;
    }

    public static ClientEncryptionOptions getClientEncryptionOptions() {
        return config.client_encryption_options;
    }

    public static String getLocalDataCenter() {
        return localDC;
    }

    public static Comparator<NetEndpoint> getLocalComparator() {
        return localComparator;
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

}
