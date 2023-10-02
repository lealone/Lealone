/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.common.util.MapUtils;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.aote.AOTransaction;

class PeriodicLogSyncService extends LogSyncService {

    private final long blockWhenSyncLagsMillis;

    PeriodicLogSyncService(Map<String, String> config) {
        super(config);
        syncIntervalMillis = MapUtils.getLong(config, "log_sync_period", 500);
        blockWhenSyncLagsMillis = (long) (syncIntervalMillis * 1.5);
    }

    @Override
    public boolean isPeriodic() {
        return true;
    }

    private boolean waitForSyncToCatchUp() {
        // 如果当前时间是第10毫秒，上次同步时间是在第5毫秒，同步间隔是10毫秒，说时当前时间还是同步周期内，就不用阻塞了
        // 如果当前时间是第16毫秒，超过了同步周期，需要阻塞
        return System.currentTimeMillis() > lastSyncedAt + blockWhenSyncLagsMillis;
    }

    @Override
    public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        PendingTransaction pt = new PendingTransaction(t, r, logId);
        // 如果在同步周期内，可以提前通知异步提交完成了
        if (!waitForSyncToCatchUp()) {
            t.onSynced(); // 不能直接pt.setSynced(true);
            asyncWrite(pt);
            pt.setCompleted(true);
            t.asyncCommitComplete();
        } else {
            asyncWrite(pt);
        }
    }

    @Override
    public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        // 如果在同步周期内，不用等
        if (!waitForSyncToCatchUp()) {
            PendingTransaction pt = new PendingTransaction(t, r, logId);
            t.onSynced();
            asyncWrite(pt);
            pt.setCompleted(true);
            // 同步调用无需t.asyncCommitComplete();
        } else {
            super.syncWrite(t, r, logId);
        }
    }
}
