/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.io.File;
import java.net.URL;
import java.util.Iterator;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.db.Constants;
import org.lealone.p2p.config.Config;

public class Utils extends org.lealone.common.util.Utils {

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

    public static String resourceToFile(String filename) throws ConfigException {
        ClassLoader loader = Utils.class.getClassLoader();
        URL scpurl = loader.getResource(filename);
        if (scpurl == null)
            throw new ConfigException("unable to locate " + filename);

        return new File(scpurl.getFile()).getAbsolutePath();
    }

    public static boolean isUnix() {
        return !Constants.IS_WINDOWS;
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
