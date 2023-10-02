/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.common.util.MapUtils;

class InstantLogSyncService extends LogSyncService {

    InstantLogSyncService(Map<String, String> config) {
        super(config);
        syncIntervalMillis = MapUtils.getLong(config, "log_sync_service_loop_interval", 100);
    }
}
