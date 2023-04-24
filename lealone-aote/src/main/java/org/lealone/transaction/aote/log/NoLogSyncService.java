/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.transaction.aote.AOTransaction;

class NoLogSyncService extends LogSyncService {

    NoLogSyncService(Map<String, String> config) {
        super(config);
    }

    @Override
    public boolean needSync() {
        return false;
    }

    @Override
    public void run() {
    }

    @Override
    public void close() {
    }

    @Override
    public void asyncCommit(RedoLogRecord r, AOTransaction t) {
        t.asyncCommitComplete();
    }

    @Override
    public void addAndMaybeWaitForSync(RedoLogRecord r) {
    }

    @Override
    public void checkpoint(long checkpointId, boolean saved) {
    }
}
