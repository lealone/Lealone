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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;

import org.lealone.db.DataBuffer;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

/**
 * A queue-based redo log chunk
 *  
 * @author zhh
 */
class RedoLogChunk implements Comparable<RedoLogChunk> {

    static final String CHUNK_FILE_NAME_PREFIX = "redoLog" + RedoLog.NAME_ID_SEPARATOR;

    private static String getChunkFileName(Map<String, String> config, int id) {
        String storagePath = config.get("storagePath");
        return storagePath + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private final int id;
    private final FileStorage fileStorage;
    private final Map<String, String> config;
    private LinkedTransferQueue<RedoLogRecord> queue;
    private LinkedTransferQueue<LazyLog> lazyLogQueue;
    private long pos;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        this.config = config;
        String chunkFileName = getChunkFileName(config, id);
        fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, config);
        queue = new LinkedTransferQueue<>();
        lazyLogQueue = new LinkedTransferQueue<>();
        pos = fileStorage.size();
        if (pos > 0)
            read();
    }

    private void read() {
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            RedoLogRecord r = RedoLogRecord.read(buffer);
            if (r.isCheckpoint())
                queue = new LinkedTransferQueue<>(); // 丢弃之前的
            queue.add(r);
        }
    }

    int getId() {
        return id;
    }

    void addRedoLogRecord(RedoLogRecord r) {
        queue.add(r);
    }

    void addLazyLog(LazyLog lazyLog) {
        lazyLogQueue.add(lazyLog);
    }

    LinkedTransferQueue<RedoLogRecord> getAndResetRedoLogRecords() {
        LinkedTransferQueue<RedoLogRecord> oldQueue = this.queue;
        this.queue = new LinkedTransferQueue<>();
        return oldQueue;
    }

    LinkedTransferQueue<LazyLog> getAndResetLazyLogs() {
        LinkedTransferQueue<LazyLog> lazyLogQueue = this.lazyLogQueue;
        this.lazyLogQueue = new LinkedTransferQueue<>();
        return lazyLogQueue;
    }

    void close() {
        save();
        fileStorage.close();
    }

    synchronized void save() {
        LinkedTransferQueue<RedoLogRecord> oldQueue = getAndResetRedoLogRecords();
        if (!oldQueue.isEmpty()) {
            try (DataBuffer buff = DataBuffer.create()) {
                for (RedoLogRecord r : oldQueue) {
                    if (r.isCheckpoint()) {
                        deleteOldChunkFiles();
                        fileStorage.truncate(0);
                        buff.reset();
                        pos = 0;
                    }
                    r.write(buff);
                }
                int chunkLength = buff.position();
                if (chunkLength > 0) {
                    buff.limit(chunkLength);
                    buff.position(0);
                    fileStorage.writeFully(pos, buff.getBuffer());
                    pos += chunkLength;
                    fileStorage.sync();
                }
                for (RedoLogRecord r : oldQueue) {
                    r.setSynced(true);
                }
            }
        }

        LinkedTransferQueue<LazyLog> lazyLogQueue = getAndResetLazyLogs();
        if (!lazyLogQueue.isEmpty()) {
            try (DataBuffer buff = DataBuffer.create()) {
                for (LazyLog log : lazyLogQueue) {
                    RedoLogRecord r = log.createRedoLogRecord(buff);
                    if (r == null)
                        continue;
                    if (r.isCheckpoint()) {
                        deleteOldChunkFiles();
                        fileStorage.truncate(0);
                        buff.reset();
                        pos = 0;
                    }
                }
                int chunkLength = buff.position();
                if (chunkLength > 0) {
                    buff.limit(chunkLength);
                    buff.position(0);
                    fileStorage.writeFully(pos, buff.getBuffer());
                    pos += chunkLength;
                    fileStorage.sync();
                }
            }
        }
    }

    private void deleteOldChunkFiles() {
        for (int i = 0; i < id; i++) {
            String chunkFileName = getChunkFileName(config, i);
            if (FileUtils.exists(chunkFileName))
                FileUtils.delete(chunkFileName);
        }
    }

    long logChunkSize() {
        return pos;
    }

    @Override
    public int compareTo(RedoLogChunk o) {
        return Integer.signum(this.id - o.id);
    }

    @Override
    public String toString() {
        return "RedoLogChunk[" + id + ", " + fileStorage.getFileName() + "]";
    }
}
