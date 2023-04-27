/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.util.Map;

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
    public void asyncWrite(RedoLogRecord r) {
        r.onSynced();
    }

    @Override
    public void syncWrite(RedoLogRecord r) {
        r.onSynced();
    }

    @Override
    public void checkpoint(boolean saved) {
    }
}
