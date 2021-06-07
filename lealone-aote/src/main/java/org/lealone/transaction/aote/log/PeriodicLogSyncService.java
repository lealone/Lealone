/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.transaction.aote.AMTransaction;

class PeriodicLogSyncService extends LogSyncService {

    private static final long DEFAULT_LOG_SYNC_PERIOD = 500;

    private final long blockWhenSyncLagsMillis;

    PeriodicLogSyncService(Map<String, String> config) {
        super(config);
        if (config.containsKey("log_sync_period"))
            syncIntervalMillis = Long.parseLong(config.get("log_sync_period"));
        else
            syncIntervalMillis = DEFAULT_LOG_SYNC_PERIOD;

        blockWhenSyncLagsMillis = (long) (syncIntervalMillis * 1.5);
    }

    @Override
    public void maybeWaitForSync(RedoLogRecord r) {
        haveWork.release();
        if (!r.isSynced()) {
            // 因为Long.MAX_VALUE > Long.MAX_VALUE + 1
            // lastSyncedAt是long类型，当lastSyncedAt为Long.MAX_VALUE时，
            // 再加一个int类型的blockWhenSyncLagsMillis时还是小于Long.MAX_VALUE；
            // 当lastSyncedAt + blockWhenSyncLagsMillis正好等于Long.MAX_VALUE时，就不阻塞了
            // 也就是这个if只有lastSyncedAt + blockWhenSyncLagsMillis正好等于Long.MAX_VALUE时才是false
            if (waitForSyncToCatchUp(Long.MAX_VALUE)) {
                // wait until periodic sync() catches up with its schedule
                long started = System.currentTimeMillis();
                while (waitForSyncToCatchUp(started)) {
                    WaitQueue.Signal signal = syncComplete.register();
                    if (r.isSynced()) {
                        signal.cancel();
                        return;
                    } else if (waitForSyncToCatchUp(started)) {
                        signal.awaitUninterruptibly();
                    } else {
                        signal.cancel();
                    }
                }
            }
        }
    }

    private boolean waitForSyncToCatchUp(long started) {
        // 如果当前时间是第10毫秒，上次同步时间是在第5毫秒，同步间隔是10毫秒，说时当前时间还是同步周期内，就不用阻塞了
        // 如果当前时间是第16毫秒，超过了同步周期，需要阻塞
        return started > lastSyncedAt + blockWhenSyncLagsMillis;
    }

    @Override
    public void asyncCommit(AMTransaction t) {
        // 如果在同步周期内，可以提前提交事务
        long started = System.currentTimeMillis();
        if (!waitForSyncToCatchUp(started)) {
            t.asyncCommitComplete();
        } else {
            super.asyncCommit(t);
        }
    }
}
