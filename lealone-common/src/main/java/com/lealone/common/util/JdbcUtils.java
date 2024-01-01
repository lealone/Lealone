/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.common.util;

/**
 * This is a utility class with JDBC helper functions.
 */
public class JdbcUtils {

    private JdbcUtils() {
        // utility class
    }

    public static void closeSilently(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
