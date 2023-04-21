/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.common.util.MapUtils;
import org.lealone.transaction.aote.AOTransaction;

class PeriodicLogSyncService extends LogSyncService {

    private final long blockWhenSyncLagsMillis;

    PeriodicLogSyncService(Map<String, String> config) {
        super(config);
        syncIntervalMillis = MapUtils.getLong(config, "log_sync_period", 500);
        blockWhenSyncLagsMillis = (long) (syncIntervalMillis * 1.5);
    }

    private boolean waitForSyncToCatchUp() {
        // 如果当前时间是第10毫秒，上次同步时间是在第5毫秒，同步间隔是10毫秒，说时当前时间还是同步周期内，就不用阻塞了
        // 如果当前时间是第16毫秒，超过了同步周期，需要阻塞
        return System.currentTimeMillis() > lastSyncedAt + blockWhenSyncLagsMillis;
    }

    @Override
    public void addAndMaybeWaitForSync(RedoLogRecord r) {
        // 如果在同步周期内，不用等
        if (!waitForSyncToCatchUp()) {
            addRedoLogRecord(r);
        } else {
            addAndWaitForSync(r);
        }
    }

    @Override
    public void asyncCommit(RedoLogRecord r, AOTransaction t) {
        // 如果在同步周期内，可以提前提交事务
        if (!waitForSyncToCatchUp()) {
            t.asyncCommitComplete();
        } else {
            super.asyncCommit(r, t);
        }
    }
}
