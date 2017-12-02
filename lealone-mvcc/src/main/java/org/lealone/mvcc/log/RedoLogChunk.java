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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;

import org.lealone.db.DataBuffer;
import org.lealone.storage.fs.FileStorage;

/**
 * A queue-based redo log chunk
 *  
 * @author zhh
 */
class RedoLogChunk implements Comparable<RedoLogChunk> {

    private static final String CHUNK_FILE_NAME_PREFIX = "redoLog" + RedoLog.NAME_ID_SEPARATOR;

    private static String getChunkFileName(Map<String, String> config, int id) {
        String storageName = config.get("storageName");
        return storageName + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private final int id;
    private final FileStorage fileStorage;

    private LinkedTransferQueue<RedoLogValue> queue;
    private long pos;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        String chunkFileName = getChunkFileName(config, id);
        fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, config);
        queue = new LinkedTransferQueue<>();
        pos = fileStorage.size();
        if (pos > 0)
            read();
    }

    private void read() {
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            RedoLogValue v = RedoLogValue.read(buffer);
            if (v.checkpoint)
                queue = new LinkedTransferQueue<>();
            else
                queue.add(v);
        }
    }

    int getId() {
        return id;
    }

    void addRedoLogValue(RedoLogValue value) {
        queue.add(value);
    }

    LinkedTransferQueue<RedoLogValue> getAndResetRedoLogValues() {
        LinkedTransferQueue<RedoLogValue> oldQueue = this.queue;
        this.queue = new LinkedTransferQueue<>();
        return oldQueue;
    }

    void close() {
        save();
        fileStorage.close();
    }

    synchronized void save() {
        LinkedTransferQueue<RedoLogValue> oldQueue = getAndResetRedoLogValues();
        if (!oldQueue.isEmpty()) {
            try (DataBuffer buff = DataBuffer.create()) {
                for (RedoLogValue v : oldQueue) {
                    v.write(buff);
                }
                int chunkLength = buff.position();
                if (chunkLength > 0) {
                    buff.limit(chunkLength);
                    buff.position(0);
                    fileStorage.writeFully(pos, buff.getBuffer());
                    pos += chunkLength;
                    fileStorage.sync();
                }
                for (RedoLogValue v : oldQueue) {
                    v.synced = true;
                }
            }
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
