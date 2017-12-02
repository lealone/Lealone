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
import java.util.Queue;

import org.lealone.db.Constants;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;

/**
 * A redo log
 *
 * @author zhh
 */
public class RedoLog {

    private static final long DEFAULT_LOG_CHUNK_SIZE = 32 * 1024 * 1024;

    public static final String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static final String LOG_SYNC_TYPE_BATCH = "batch";
    public static final String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    public static final char NAME_ID_SEPARATOR = Constants.NAME_SEPARATOR;

    private final Map<String, String> config;
    private final long logChunkSize;
    private final LogSyncService logSyncService;

    private RedoLogChunk current;

    public RedoLog(Map<String, String> config) {
        this.config = config;
        if (config.containsKey("log_chunk_size"))
            logChunkSize = Long.parseLong(config.get("log_chunk_size"));
        else
            logChunkSize = DEFAULT_LOG_CHUNK_SIZE;

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

        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new PeriodicLogSyncService(config);
        else if (LOG_SYNC_TYPE_BATCH.equalsIgnoreCase(logSyncType))
            logSyncService = new BatchLogSyncService(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoLogSyncService();
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);
        logSyncService.setRedoLog(this);

        current = new RedoLogChunk(lastId, config);
    }

    public void addRedoLogRecord(RedoLogRecord r) {
        current.addRedoLogRecord(r);
    }

    public Queue<RedoLogRecord> getAndResetRedoLogRecords() {
        return current.getAndResetRedoLogRecords();
    }

    public void close() {
        save();
        current.close();

        logSyncService.close();
        try {
            logSyncService.join();
        } catch (InterruptedException e) {
        }
    }

    public void save() {
        current.save();
        if (current.logChunkSize() > logChunkSize) {
            current.close();
            current = new RedoLogChunk(current.getId() + 1, config);
        }
    }

    public LogSyncService getLogSyncService() {
        return logSyncService;
    }

    public void writeCheckpoint() {
        RedoLogRecord r = new RedoLogRecord(true);
        addRedoLogRecord(r);
        logSyncService.maybeWaitForSync(r);
    }

}
