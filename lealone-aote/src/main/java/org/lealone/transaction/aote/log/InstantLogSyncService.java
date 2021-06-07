/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.common.util.DateTimeUtils;

class InstantLogSyncService extends LogSyncService {

    private static final long DEFAULT_LOG_SYNC_INTERVAL = 5;

    InstantLogSyncService(Map<String, String> config) {
        super(config);
        syncIntervalMillis = DateTimeUtils.getLoopInterval(config, "log_sync_service_loop_interval",
                DEFAULT_LOG_SYNC_INTERVAL);
    }

    @Override
    public boolean isInstantSync() {
        return true;
    }

    @Override
    public void maybeWaitForSync(RedoLogRecord r) {
        haveWork.release();
        if (!r.isSynced() && running) {
            while (true) {
                WaitQueue.Signal signal = syncComplete.register();
                if (r.isSynced() || !running) {
                    signal.cancel();
                    return;
                } else
                    signal.awaitUninterruptibly();
            }
        }
    }
}
