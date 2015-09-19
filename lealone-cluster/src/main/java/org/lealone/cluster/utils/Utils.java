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
package org.lealone.cluster.utils;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.dht.IPartitioner;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final BigInteger TWO = new BigInteger("2");

    private static final boolean isWindows = System.getProperty("os.name").startsWith("Windows");

    private static volatile InetAddress localInetAddress;
    private static volatile InetAddress broadcastInetAddress;

    public static final int MAX_UNSIGNED_SHORT = 0xFFFF;

    private static String JMX_OBJECT_NAME_PREFIX = "org.lealone.cluster:type=";

    public static String getJmxObjectName(String type) {
        return JMX_OBJECT_NAME_PREFIX + type;
    }

    public static int getAvailableProcessors() {
        if (Config.getProperty("available.processors") != null)
            return Integer.parseInt(Config.getProperty("available.processors"));
        else
            return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Please use getBroadcastAddress instead. You need this only when you have to listen/connect.
     */
    public static InetAddress getLocalAddress() {
        if (localInetAddress == null)
            try {
                localInetAddress = DatabaseDescriptor.getListenAddress() == null ? InetAddress.getLocalHost()
                        : DatabaseDescriptor.getListenAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        return localInetAddress;
    }

    public static InetAddress getBroadcastAddress() {
        if (broadcastInetAddress == null)
            broadcastInetAddress = DatabaseDescriptor.getBroadcastAddress() == null ? getLocalAddress()
                    : DatabaseDescriptor.getBroadcastAddress();
        return broadcastInetAddress;
    }

    public static Collection<InetAddress> getAllLocalAddresses() {
        Set<InetAddress> localAddresses = new HashSet<InetAddress>();
        try {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            if (nets != null) {
                while (nets.hasMoreElements())
                    localAddresses.addAll(Collections.list(nets.nextElement().getInetAddresses()));
            }
        } catch (SocketException e) {
            throw new AssertionError(e);
        }
        return localAddresses;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger, Boolean> midpoint(BigInteger left, BigInteger right, int sigbits) {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0) {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        } else {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return Pair.create(midpoint, remainder);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2, int offset1, int offset2, int len1, int len2) {
        return FastByteOperations.compareUnsigned(bytes1, offset1, len1, bytes2, offset2, len2);
    }

    public static int compareUnsigned(byte[] bytes1, byte[] bytes2) {
        return compareUnsigned(bytes1, bytes2, 0, 0, bytes1.length, bytes2.length);
    }

    public static String resourceToFile(String filename) throws ConfigurationException {
        ClassLoader loader = Utils.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return new File(scpurl.getFile()).getAbsolutePath();
    }

    public static String getReleaseVersionString() {
        InputStream in = null;
        try {
            in = Utils.class.getClassLoader().getResourceAsStream("org/lealone/res/version.properties");
            if (in == null) {
                return Config.getProperty("release.version", "Unknown");
            }
            Properties props = new Properties();
            props.load(in);
            return props.getProperty("lealoneVersion");
        } catch (Exception e) {
            JVMStabilityInspector.inspectThrowable(e);
            logger.warn("Unable to load version.properties", e);
            return "debug version";
        } finally {
            FileUtils.closeQuietly(in);
        }
    }

    public static IPartitioner newPartitioner(String partitionerClassName) throws ConfigurationException {
        if (!partitionerClassName.contains("."))
            partitionerClassName = IPartitioner.class.getPackage().getName() + "." + partitionerClassName;
        return Utils.instanceOrConstruct(partitionerClassName, "partitioner");
    }

    /**
     * @return The Class for the given name.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    @SuppressWarnings("unchecked")
    public static <T> Class<T> classForName(String classname, String readable) throws ConfigurationException {
        try {
            return (Class<T>) Class.forName(classname);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new ConfigurationException(String.format("Unable to find %s class '%s'", readable, classname), e);
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T instanceOrConstruct(String classname, String readable) throws ConfigurationException {
        Class<T> cls = Utils.classForName(classname, readable);
        try {
            Field instance = cls.getField("instance");
            return cls.cast(instance.get(null));
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            // Could not get instance field. Try instantiating.
            return construct(cls, classname, readable);
        }
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T construct(String classname, String readable) throws ConfigurationException {
        Class<T> cls = Utils.classForName(classname, readable);
        return construct(cls, classname, readable);
    }

    private static <T> T construct(Class<T> cls, String classname, String readable) throws ConfigurationException {
        try {
            return cls.newInstance();
        } catch (IllegalAccessException e) {
            throw new ConfigurationException(String.format("Default constructor for %s class '%s' is inaccessible.",
                    readable, classname));
        } catch (InstantiationException e) {
            throw new ConfigurationException(
                    String.format("Cannot use abstract class '%s' as %s.", classname, readable));
        } catch (Exception e) {
            // Catch-all because Class.newInstance()
            // "propagates any exception thrown by the nullary constructor, including a checked exception".
            if (e.getCause() instanceof ConfigurationException)
                throw (ConfigurationException) e.getCause();
            throw new ConfigurationException(String.format("Error instantiating %s class '%s'.", readable, classname),
                    e);
        }
    }

    public static String toString(Map<?, ?> map) {
        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(":");
        return joiner.join(map);
    }

    public static boolean isUnix() {
        return !isWindows;
    }
}
