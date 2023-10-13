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
    public void asyncWakeUp() {
    }

    @Override
    public void asyncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        t.onSynced();
        t.asyncCommitComplete();
    }

    @Override
    public void syncWrite(AOTransaction t, RedoLogRecord r, long logId) {
        t.onSynced();
    }

    @Override
    public void addRedoLogRecord(AOTransaction t, RedoLogRecord r, long logId) {
    }

}
