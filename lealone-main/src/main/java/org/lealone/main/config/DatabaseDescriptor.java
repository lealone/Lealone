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
package org.lealone.main.config;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Comparator;
import java.util.Enumeration;

import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.common.util.Utils;

public class DatabaseDescriptor {

    private static Config conf;

    private static InetAddress listenAddress; // leave null so we can fall through to getLocalHost
    private static InetAddress broadcastAddress;
    private static InetAddress broadcastRpcAddress;

    private static String localDC;
    private static Comparator<InetAddress> localComparator;

    public static Config loadConfig() throws ConfigurationException {
        if (conf != null)
            return conf;

        String loaderClass = Config.getProperty("config.loader");
        ConfigurationLoader loader = loaderClass == null ? new YamlConfigurationLoader() : Utils
                .<ConfigurationLoader> construct(loaderClass, "configuration loading");
        Config conf = loader.loadConfig();

        applyConfig(conf);
        return conf;
    }

    private static void applyConfig(Config config) throws ConfigurationException {
        conf = config;

        applyAddressConfig(config);
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
                throw new ConfigurationException("listen_address cannot be a wildcard address (" + conf.listen_address
                        + ")!");
        } else if (conf.listen_interface != null) {
            listenAddress = getNetworkInterfaceAddress(conf.listen_interface, "listen_interface",
                    conf.listen_interface_prefer_ipv6);
        } else {
            try {
                listenAddress = InetAddress.getLocalHost();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
            conf.listen_address = listenAddress.getHostAddress();
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
                throw new ConfigurationException("Configured " + configName + " \"" + intf
                        + "\" was found, but had no addresses");
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

    public static String getLocalDataCenter() {
        return localDC;
    }

    public static Comparator<InetAddress> getLocalComparator() {
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
