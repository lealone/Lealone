/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.util;

import java.util.Map;

public class MapUtils {

    private MapUtils() {
        // utility class
    }

    public static int getInt(Map<String, String> map, String key, int def) {
        if (map == null)
            return def;
        String value = map.get(key);
        return Utils.toInt(value, def);
    }

    public static long getLongMB(Map<String, String> map, String key, long def) {
        if (map == null)
            return def;
        String value = map.get(key);
        return Utils.toLongMB(value, def);
    }

    public static long getLong(Map<String, String> map, String key, long def) {
        if (map == null)
            return def;
        String value = map.get(key);
        return Utils.toLong(value, def);
    }

    public static boolean getBoolean(Map<String, String> map, String key, boolean def) {
        if (map == null)
            return def;
        String value = map.get(key);
        return Utils.toBoolean(value, def);
    }

    public static String getString(Map<String, String> map, String key, String def) {
        if (map == null)
            return def;
        String value = map.get(key);
        if (value == null)
            return def;
        else
            return value;
    }

    public static int getSchedulerCount(Map<String, String> map) {
        if (map != null && map.containsKey("scheduler_count"))
            return Math.max(1, Integer.parseInt(map.get("scheduler_count")));
        else
            return Runtime.getRuntime().availableProcessors();
    }
}
