/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lealone.common.util.MapUtils;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

public class RedoLog {

    // key: mapName, value: map key/value ByteBuffer list
    private final HashMap<String, List<ByteBuffer>> pendingRedoLog = new HashMap<>();
    private final Map<String, String> config;
    private final long logChunkSize;

    private RedoLogChunk currentChunk;

    RedoLog(Map<String, String> config) {
        this.config = config;
        logChunkSize = MapUtils.getLong(config, "log_chunk_size", 32 * 1024 * 1024); // 默认32M

        String baseDir = config.get("base_dir");
        String logDir = config.get("redo_log_dir");
        String storagePath = baseDir + File.separator + logDir;
        config.put(StorageSetting.STORAGE_PATH.name(), storagePath);

        if (!FileUtils.exists(storagePath))
            FileUtils.createDirectories(storagePath);
    }

    private List<Integer> getAllChunkIds() {
        ArrayList<Integer> ids = new ArrayList<>();
        int prefixLength = RedoLogChunk.CHUNK_FILE_NAME_PREFIX.length();
        FilePath dir = FilePath.get(config.get(StorageSetting.STORAGE_PATH.name()));
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
                    for (RedoLogRecord r : chunk.readRedoLogRecords()) {
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
                    TransactionalValue tv = TransactionalValue.createCommitted(value);
                    map.put(key, tv);
                }
            }
        }
    }

    int logQueueSize() {
        return currentChunk.logQueueSize();
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

    public void ignoreCheckpoint() {
        currentChunk.ignoreCheckpoint();
    }
}
