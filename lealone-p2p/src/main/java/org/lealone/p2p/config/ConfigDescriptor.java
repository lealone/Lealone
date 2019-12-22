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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.Config.ClusterConfig;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.p2p.locator.AbstractNodeAssignmentStrategy;
import org.lealone.p2p.locator.AbstractReplicationStrategy;
import org.lealone.p2p.locator.DynamicNodeSnitch;
import org.lealone.p2p.locator.INodeSnitch;
import org.lealone.p2p.locator.NodeSnitchInfo;
import org.lealone.p2p.locator.RandomNodeAssignmentStrategy;
import org.lealone.p2p.locator.SeedProvider;
import org.lealone.p2p.locator.SimpleStrategy;
import org.lealone.p2p.server.P2pServerEngine;
import org.lealone.p2p.util.Utils;
import org.lealone.server.protocol.PacketType;

public class ConfigDescriptor {

    private static Config config;
    private static NetNode localP2pNode;
    private static INodeSnitch snitch;
    private static String localDC;
    private static Comparator<NetNode> localComparator;
    private static SeedProvider seedProvider;
    private static AbstractReplicationStrategy defaultReplicationStrategy;
    private static AbstractNodeAssignmentStrategy defaultNodeAssignmentStrategy;

    public static void applyConfig(Config config) throws ConfigException {
        ConfigDescriptor.config = config;

        // 单机模式下不需要加载集群相关的配置，
        // 避免创建不必要的资源，例如实例化DynamicNodeSnitch时需要开启ScheduledTasks线程
        if (!isP2pServerEnabled())
            return;

        // phi convict threshold for FailureDetector
        if (config.cluster_config.phi_convict_threshold < 5 || config.cluster_config.phi_convict_threshold > 16) {
            throw new ConfigException("phi_convict_threshold must be between 5 and 16");
        }

        localP2pNode = createLocalP2pNode(config);
        snitch = createNodeSnitch(config.cluster_config);

        localDC = snitch.getDatacenter(localP2pNode);
        localComparator = new Comparator<NetNode>() {
            @Override
            public int compare(NetNode node1, NetNode node2) {
                boolean local1 = localDC.equals(snitch.getDatacenter(node1));
                boolean local2 = localDC.equals(snitch.getDatacenter(node2));
                if (local1 && !local2)
                    return -1;
                if (local2 && !local1)
                    return 1;
                return 0;
            }
        };

        seedProvider = createSeedProvider(config.cluster_config);
        defaultReplicationStrategy = createDefaultReplicationStrategy(config.cluster_config);
        defaultNodeAssignmentStrategy = createDefaultNodeAssignmentStrategy(config.cluster_config);
    }

    private static boolean isP2pServerEnabled() {
        boolean p2pServerEnabled = false;
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (P2pServerEngine.NAME.equalsIgnoreCase(e.name)) {
                p2pServerEnabled = e.enabled;
            }
        }
        return p2pServerEnabled;
    }

    private static NetNode createLocalP2pNode(Config config) throws ConfigException {
        InetAddress listenAddress = null;
        // Local IP, hostname or interface to bind services to
        if (config.listen_address != null && config.listen_interface != null) {
            throw new ConfigException("Set listen_address OR listen_interface, not both");
        } else if (config.listen_address != null) {
            try {
                listenAddress = InetAddress.getByName(config.listen_address);
            } catch (UnknownHostException e) {
                throw new ConfigException("Unknown listen_address '" + config.listen_address + "'");
            }
            if (listenAddress.isAnyLocalAddress())
                throw new ConfigException(
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
        return new NetNode(host, port);
    }

    private static InetAddress getNetworkInterfaceAddress(String intf, String configName, boolean preferIPv6)
            throws ConfigException {
        try {
            NetworkInterface ni = NetworkInterface.getByName(intf);
            if (ni == null)
                throw new ConfigException("Configured " + configName + " \"" + intf + "\" could not be found");
            Enumeration<InetAddress> addrs = ni.getInetAddresses();
            if (!addrs.hasMoreElements())
                throw new ConfigException(
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
            throw new ConfigException("Configured " + configName + " \"" + intf + "\" caused an exception", e);
        }
    }

    private static INodeSnitch createNodeSnitch(ClusterConfig config) throws ConfigException {
        // end point snitch
        if (config.node_snitch == null) {
            throw new ConfigException("Missing node_snitch directive");
        }

        String className = config.node_snitch;
        if (!className.contains("."))
            className = INodeSnitch.class.getPackage().getName() + "." + className;
        INodeSnitch snitch = Utils.construct(className, "snitch");
        if (config.dynamic_snitch)
            snitch = new DynamicNodeSnitch(snitch);

        NodeSnitchInfo.create();
        return snitch;
    }

    private static SeedProvider createSeedProvider(ClusterConfig config) throws ConfigException {
        if (config.seed_provider == null) {
            throw new ConfigException("seeds configuration is missing; a minimum of one seed is required.");
        }
        if (config.seed_provider.name == null) {
            throw new ConfigException("seed_provider.name is missing.");
        }
        SeedProvider seedProvider;
        String className = config.seed_provider.name;
        if (!className.contains("."))
            className = SeedProvider.class.getPackage().getName() + "." + className;
        try {
            Class<?> seedProviderClass = Class.forName(className);
            seedProvider = (SeedProvider) seedProviderClass.getConstructor(Map.class)
                    .newInstance(config.seed_provider.parameters);
        }
        // there are about 5 checked exceptions that could be thrown here.
        catch (Exception e) {
            throw new ConfigException(
                    e.getMessage() + "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
        if (seedProvider.getSeeds().isEmpty())
            throw new ConfigException("The seed provider lists no seeds.");
        return seedProvider;
    }

    private static AbstractReplicationStrategy createDefaultReplicationStrategy(ClusterConfig config)
            throws ConfigException {
        AbstractReplicationStrategy defaultReplicationStrategy;
        if (config.replication_strategy == null) {
            HashMap<String, String> map = new HashMap<>(1);
            map.put("replication_factor", "1");
            defaultReplicationStrategy = new SimpleStrategy("system", getNodeSnitch(), map);
        } else {
            if (config.replication_strategy.name == null) {
                throw new ConfigException("replication_strategy.name is missing.");
            }
            defaultReplicationStrategy = AbstractReplicationStrategy.create("system", config.replication_strategy.name,
                    getNodeSnitch(), config.replication_strategy.parameters, true);
        }
        return defaultReplicationStrategy;
    }

    private static AbstractNodeAssignmentStrategy createDefaultNodeAssignmentStrategy(ClusterConfig config)
            throws ConfigException {
        AbstractNodeAssignmentStrategy defaultNodeAssignmentStrategy;
        if (config.node_assignment_strategy == null) {
            HashMap<String, String> map = new HashMap<>(1);
            map.put("assignment_factor", "1");
            defaultNodeAssignmentStrategy = new RandomNodeAssignmentStrategy("system", getNodeSnitch(), map);
        } else {
            if (config.node_assignment_strategy.name == null) {
                throw new ConfigException("node_assignment_strategy.name is missing.");
            }
            defaultNodeAssignmentStrategy = AbstractNodeAssignmentStrategy.create("system",
                    config.node_assignment_strategy.name, getNodeSnitch(), config.node_assignment_strategy.parameters,
                    true);
        }
        return defaultNodeAssignmentStrategy;
    }

    public static AbstractReplicationStrategy getDefaultReplicationStrategy() {
        return defaultReplicationStrategy;
    }

    public static AbstractNodeAssignmentStrategy getDefaultNodeAssignmentStrategy() {
        return defaultNodeAssignmentStrategy;
    }

    public static INodeSnitch getNodeSnitch() {
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

    // not part of the PacketType enum so we can change timeouts easily via JMX
    public static long getTimeout(PacketType packetType) {
        return getRpcTimeout();
    }

    public static double getPhiConvictThreshold() {
        return config.cluster_config.phi_convict_threshold;
    }

    public static void setPhiConvictThreshold(double phiConvictThreshold) {
        config.cluster_config.phi_convict_threshold = phiConvictThreshold;
    }

    public static Set<NetNode> getSeeds() {
        return new HashSet<>(seedProvider.getSeeds());
    }

    public static List<NetNode> getSeedList() {
        return seedProvider.getSeeds();
    }

    public static NetNode getLocalNode() {
        return localP2pNode;
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

    public static Comparator<NetNode> getLocalComparator() {
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
