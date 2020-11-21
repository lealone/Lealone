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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.storage.StorageMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

/**
 * A redo log
 *
 * @author zhh
 */
public class RedoLog {

    private static final long DEFAULT_LOG_CHUNK_SIZE = 32 * 1024 * 1024;

    public static final char NAME_ID_SEPARATOR = Constants.NAME_SEPARATOR;

    // key: mapName, value: map key/value ByteBuffer list
    private final HashMap<String, List<ByteBuffer>> pendingRedoLog = new HashMap<>();
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

    public long init() {
        long lastTransactionId = 0;
        List<Integer> ids = getAllChunkIds();
        if (ids.isEmpty()) {
            currentChunk = new RedoLogChunk(0, config);
        } else {
            int lastId = ids.get(ids.size() - 1);
            for (int id : ids) {
                RedoLogChunk chunk = null;
                try {
                    chunk = new RedoLogChunk(id, config);
                    for (RedoLogRecord r : chunk.getAndResetRedoLogRecords()) {
                        lastTransactionId = r.initPendingRedoLog(pendingRedoLog, lastTransactionId);
                    }
                } finally {
                    // 注意一定要关闭，否则对应的chunk文件将无法删除，
                    // 内部会打开一个FileStorage，不会因为没有引用到了而自动关闭
                    if (id == lastId)
                        currentChunk = chunk;
                    else if (chunk != null)
                        chunk.close();
                }
            }
        }
        return lastTransactionId;
    }

    // 第一次打开底层存储的map时调用这个方法，重新执行一次上次已经成功并且在检查点之后的事务操作
    @SuppressWarnings("unchecked")
    public <K> void redo(StorageMap<K, TransactionalValue> map) {
        List<ByteBuffer> pendingKeyValues = pendingRedoLog.remove(map.getName());
        if (pendingKeyValues != null && !pendingKeyValues.isEmpty()) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = ((TransactionalValueType) map.getValueType()).valueType;
            for (ByteBuffer kv : pendingKeyValues) {
                K key = (K) kt.read(kv);
                if (kv.get() == 0)
                    map.remove(key);
                else {
                    Object value = vt.read(kv);
                    // 需要返回引用，否则无法在修改和删除时使用CAS
                    TransactionalValue ref = TransactionalValue.createRef(TransactionalValue.createCommitted(value));
                    map.put(key, ref);
                }
            }
        }
    }

    int size() {
        return currentChunk.size();
    }

    void addRedoLogRecord(RedoLogRecord r) {
        currentChunk.addRedoLogRecord(r);
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
