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
package org.lealone.aose.util;

import java.io.File;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.lealone.aose.config.Config;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.db.Constants;

import com.google.common.base.Joiner;

public class Utils extends org.lealone.common.util.Utils {
    private static final BigInteger TWO = new BigInteger("2");

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
                localInetAddress = ConfigDescriptor.getListenAddress() == null ? InetAddress.getLocalHost()
                        : ConfigDescriptor.getListenAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        return localInetAddress;
    }

    public static InetAddress getBroadcastAddress() {
        if (broadcastInetAddress == null)
            broadcastInetAddress = ConfigDescriptor.getBroadcastAddress() == null ? getLocalAddress()
                    : ConfigDescriptor.getBroadcastAddress();
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

    public static String resourceToFile(String filename) throws ConfigurationException {
        ClassLoader loader = Utils.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + filename);

        return new File(scpurl.getFile()).getAbsolutePath();
    }

    /**
     * Constructs an instance of the given class, which must have a no-arg or default constructor.
     * @param classname Fully qualified classname.
     * @param readable Descriptive noun for the role the class plays.
     * @throws ConfigurationException If the class cannot be found.
     */
    public static <T> T instanceOrConstruct(String classname, String readable) throws ConfigurationException {
        Class<T> cls = classForName(classname, readable);
        try {
            Field instance = cls.getField("instance");
            return cls.cast(instance.get(null));
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
            // Could not get instance field. Try instantiating.
            return construct(cls, classname, readable);
        }
    }

    public static String toString(Map<?, ?> map) {
        Joiner.MapJoiner joiner = Joiner.on(", ").withKeyValueSeparator(":");
        return joiner.join(map);
    }

    public static boolean isUnix() {
        return !Constants.IS_WINDOWS;
    }

    /**
     * <p>Removes a substring only if it is at the end of a source string,
     * otherwise returns the source string.</p>
     *
     * <p>A {@code null} source string will return {@code null}.
     * An empty ("") source string will return the empty string.
     * A {@code null} search string will return the source string.</p>
     *
     * <pre>
     * StringUtils.removeEnd(null, *)      = null
     * StringUtils.removeEnd("", *)        = ""
     * StringUtils.removeEnd(*, null)      = *
     * StringUtils.removeEnd("www.domain.com", ".com.")  = "www.domain.com"
     * StringUtils.removeEnd("www.domain.com", ".com")   = "www.domain"
     * StringUtils.removeEnd("www.domain.com", "domain") = "www.domain.com"
     * StringUtils.removeEnd("abc", "")    = "abc"
     * </pre>
     *
     * @param str  the source String to search, may be null
     * @param remove  the String to search for and remove, may be null
     * @return the substring with the string removed if found,
     *  {@code null} if null String input
     * @since 2.1
     */
    public static String removeEnd(String str, String remove) {
        if (isEmpty(str) || isEmpty(remove)) {
            return str;
        }
        if (str.endsWith(remove)) {
            return str.substring(0, str.length() - remove.length());
        }
        return str;
    }

    public static boolean isEmpty(CharSequence cs) {
        return cs == null || cs.length() == 0;
    }

    /**
    * <p>Joins the elements of the provided {@code Iterable} into
    * a single String containing the provided elements.</p>
    *
    * <p>No delimiter is added before or after the list.
    * A {@code null} separator is the same as an empty String ("").</p>
    *
    * <p>See the examples here: {@link #join(Object[],String)}. </p>
    *
    * @param iterable  the {@code Iterable} providing the values to join together, may be null
    * @param separator  the separator character to use, null treated as ""
    * @return the joined String, {@code null} if null iterator input
    * @since 2.3
    */
    public static String join(Iterable<?> iterable, String separator) {
        if (iterable == null) {
            return null;
        }
        return join(iterable.iterator(), separator);
    }

    /**
     * <p>Joins the elements of the provided {@code Iterator} into
     * a single String containing the provided elements.</p>
     *
     * <p>No delimiter is added before or after the list.
     * A {@code null} separator is the same as an empty String ("").</p>
     *
     * <p>See the examples here: {@link #join(Object[],String)}. </p>
     *
     * @param iterator  the {@code Iterator} of values to join together, may be null
     * @param separator  the separator character to use, null treated as ""
     * @return the joined String, {@code null} if null iterator input
     */
    public static String join(Iterator<?> iterator, String separator) {

        // handle null, zero and one elements before building a buffer
        if (iterator == null) {
            return null;
        }
        if (!iterator.hasNext()) {
            return "";
        }
        Object first = iterator.next();
        if (!iterator.hasNext()) {
            return toString(first);
        }

        // two or more elements
        StringBuilder buf = new StringBuilder(256); // Java default is 16, probably too small
        if (first != null) {
            buf.append(first);
        }

        while (iterator.hasNext()) {
            if (separator != null) {
                buf.append(separator);
            }
            Object obj = iterator.next();
            if (obj != null) {
                buf.append(obj);
            }
        }
        return buf.toString();
    }

    public static String join(Object[] array, char separator) {
        if (array == null) {
            return null;
        }

        return join(array, separator, 0, array.length);
    }

    public static String join(Object[] array, char separator, int startIndex, int endIndex) {
        if (array == null) {
            return null;
        }
        int noOfItems = endIndex - startIndex;
        if (noOfItems <= 0) {
            return "";
        }

        StringBuilder buf = new StringBuilder(noOfItems * 16);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                buf.append(array[i]);
            }
        }
        return buf.toString();
    }

    public static String toString(Object obj) {
        return obj == null ? "" : obj.toString();
    }
}
