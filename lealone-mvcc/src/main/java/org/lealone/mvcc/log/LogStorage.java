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

import java.io.File;
import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;

/**
 * A log storage
 * 
 * @author zhh
 */
public class LogStorage {

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_BATCH = "batch";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    public static final char NAME_ID_SEPARATOR = Constants.NAME_SEPARATOR;

    private final RedoLog redoLog;
    private final LogSyncService logSyncService;

    public LogStorage(Map<String, String> config) {
        String baseDir = config.get("base_dir");
        String logDir = config.get("transaction_log_dir");
        String storageName = baseDir + File.separator + logDir;
        config.put("storageName", storageName);

        if (!FileUtils.exists(storageName))
            FileUtils.createDirectories(storageName);

        FilePath dir = FilePath.get(storageName);
        int lastId = 0;
        for (FilePath fp : dir.newDirectoryStream()) {
            String fullName = fp.getName();
            int idStartPos = fullName.lastIndexOf(NAME_ID_SEPARATOR);
            if (idStartPos > 0) {
                int id = Integer.parseInt(fullName.substring(idStartPos + 1));
                if (id > lastId)
                    lastId = id;
            }
        }
        redoLog = new RedoLog(lastId, config);

        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new PeriodicLogSyncService(config);
        else if (LOG_SYNC_TYPE_BATCH.equalsIgnoreCase(logSyncType))
            logSyncService = new BatchLogSyncService(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoLogSyncService();
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);

        logSyncService.setRedoLog(redoLog);
        logSyncService.start();
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    public RedoLog getRedoLog() {
        return redoLog;
    }

    public synchronized void close() {
        redoLog.close();
        logSyncService.close();
        try {
            logSyncService.join();
        } catch (InterruptedException e) {
        }
    }

}
