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
package org.lealone.mvcc.log;

import java.util.Map;

import org.lealone.common.concurrent.WaitQueue;
import org.lealone.mvcc.log.LogSyncService;
import org.lealone.mvcc.log.RedoLog;

class BatchLogSyncService extends LogSyncService {
    private static final long DEFAULT_LOG_SYNC_BATCH_WINDOW = 5;

    public BatchLogSyncService(Map<String, String> config) {
        super("BatchLogSyncService");

        if (config.containsKey("log_sync_batch_window"))
            syncIntervalMillis = Long.parseLong(config.get("log_sync_batch_window"));
        else
            syncIntervalMillis = DEFAULT_LOG_SYNC_BATCH_WINDOW;
    }

    @Override
    public void maybeWaitForSync(RedoLog redoLog, Long lastOperationId) {
        haveWork.release();
        Long lastSyncKey = redoLog.getLastSyncKey();

        if (lastSyncKey == null || lastSyncKey < lastOperationId) {
            while (true) {
                WaitQueue.Signal signal = syncComplete.register();
                lastSyncKey = redoLog.getLastSyncKey();
                if (lastSyncKey != null && lastSyncKey >= lastOperationId) {
                    signal.cancel();
                    return;
                } else
                    signal.awaitUninterruptibly();
            }
        }
    }
}
