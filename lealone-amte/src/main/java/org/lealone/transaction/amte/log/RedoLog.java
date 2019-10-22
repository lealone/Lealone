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
package org.lealone.transaction.amte.log;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;

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

        List<Integer> ids = getAllChunkIds();
        int lastId;
        if (!ids.isEmpty())
            lastId = ids.get(ids.size() - 1);
        else
            lastId = 0;
        currentChunk = new RedoLogChunk(lastId, config);
    }

    private List<Integer> getAllChunkIds() {
        ArrayList<Integer> ids = new ArrayList<>();
        int prefixLength = RedoLogChunk.CHUNK_FILE_NAME_PREFIX.length();
        FilePath dir = FilePath.get(config.get("storagePath"));
        for (FilePath fp : dir.newDirectoryStream()) {
            String fullName = fp.getName();
            if (fullName.startsWith(RedoLogChunk.CHUNK_FILE_NAME_PREFIX)) {
                int id = Integer.parseInt(fullName.substring(prefixLength));
                ids.add(id);
            }
        }
        Collections.sort(ids); // 必须排序，按id从小到大的顺序读取文件，才能正确的redo
        return ids;
    }

    int size() {
        return currentChunk.size();
    }

    void addRedoLogRecord(RedoLogRecord r) {
        currentChunk.addRedoLogRecord(r);
    }

    Queue<RedoLogRecord> getAllRedoLogRecords() {
        LinkedTransferQueue<RedoLogRecord> queue = new LinkedTransferQueue<>();
        List<Integer> ids = getAllChunkIds();
        for (int id : ids) {
            RedoLogChunk chunk;
            if (id == currentChunk.getId()) {
                chunk = currentChunk;
            } else {
                chunk = new RedoLogChunk(id, config);
            }
            queue.addAll(chunk.getAndResetRedoLogRecords());
        }
        return queue;
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
