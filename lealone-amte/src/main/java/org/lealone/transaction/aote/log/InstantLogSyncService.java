/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
