/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

import org.lealone.transaction.aote.AMTransaction;

class NoLogSyncService extends LogSyncService {

    NoLogSyncService(Map<String, String> config) {
        super(config);
    }

    @Override
    public void start() {
    }

    @Override
    public void run() {
    }

    @Override
    public boolean needSync() {
        return false;
    }

    @Override
    public void maybeWaitForSync(RedoLogRecord r) {
    }

    @Override
    public void asyncCommit(AMTransaction t) {
        t.asyncCommitComplete();
    }
}
