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
package org.lealone.transaction.mvcc.log;

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
class RedoLog {

    private static final long DEFAULT_LOG_CHUNK_SIZE = 32 * 1024 * 1024;

    public static final char NAME_ID_SEPARATOR = Constants.NAME_SEPARATOR;

    private final Map<String, String> config;
    private final long logChunkSize;

    private RedoLogChunk currentChunk;

    RedoLog(Map<String, String> config) {
        this.config = config;
        if (config.containsKey("log_chunk_size"))
            logChunkSize = Long.parseLong(config.get("log_chunk_size"));
        else
            logChunkSize = DEFAULT_LOG_CHUNK_SIZE;

        String baseDir = config.get("base_dir");
        String logDir = config.get("redo_log_dir");
        String storagePath = baseDir + File.separator + logDir;
        config.put("storagePath", storagePath);

        if (!FileUtils.exists(storagePath))
            FileUtils.createDirectories(storagePath);

        FilePath dir = FilePath.get(storagePath);
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

        currentChunk = new RedoLogChunk(lastId, config);
    }

    void addRedoLogRecord(RedoLogRecord r) {
        currentChunk.addRedoLogRecord(r);
    }

    Queue<RedoLogRecord> getAndResetRedoLogRecords() {
        return currentChunk.getAndResetRedoLogRecords();
    }

    void close() {
        save();
        currentChunk.close();
    }

    void save() {
        currentChunk.save();
        if (currentChunk.logChunkSize() > logChunkSize) {
            currentChunk.close();
            currentChunk = new RedoLogChunk(currentChunk.getId() + 1, config);
        }
    }
}
