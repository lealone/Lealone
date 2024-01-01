/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import java.util.Map;

import com.lealone.common.util.MapUtils;
import com.lealone.common.util.SystemPropertyUtils;

public enum RunMode {
    EMBEDDED,
    CLIENT_SERVER,
    REPLICATION,
    SHARDING;

    public static boolean isEmbedded(Map<String, String> config) {
        return MapUtils.getBoolean(config, "embedded", false)
                || SystemPropertyUtils.getBoolean("lealone.embedded", false);
    }
}
