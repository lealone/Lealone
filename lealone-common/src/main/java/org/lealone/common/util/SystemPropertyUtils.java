/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

public class SystemPropertyUtils {

    private SystemPropertyUtils() {
        // utility class
    }

    public static int getInt(String key, int def) {
        String value = System.getProperty(key);
        return Utils.toInt(value, def);
    }
}
