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
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.StorageMap;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.StorageDataType;
import org.lealone.transaction.aote.CheckpointService;
import org.lealone.transaction.aote.CheckpointService.FsyncTask;
import org.lealone.transaction.aote.TransactionalValue;
import org.lealone.transaction.aote.TransactionalValueType;

public class RedoLog {

    // key: mapName, value: map key/value ByteBuffer list
    private final HashMap<String, List<ByteBuffer>> pendingRedoLog = new HashMap<>();
    private final Map<String, String> config;
    private final LogSyncService logSyncService;

    private RedoLogChunk currentChunk;

    RedoLog(Map<String, String> config, LogSyncService logSyncService) {
        this.config = config;
        this.logSyncService = logSyncService;

        String baseDir = config.get("base_dir");
        String logDir = MapUtils.getString(config, "redo_log_dir", "redo_log");
        String storagePath = baseDir + File.separator + logDir;
        config.put(StorageSetting.STORAGE_PATH.name(), storagePath);

        if (!FileUtils.exists(storagePath))
            FileUtils.createDirectories(storagePath);
    }

    private List<Integer> getAllChunkIds() {
        return getAllChunkIds(config.get(StorageSetting.STORAGE_PATH.name()));
    }

    static List<Integer> getAllChunkIds(String dirStr) {
        ArrayList<Integer> ids = new ArrayList<>();
        int prefixLength = RedoLogChunk.CHUNK_FILE_NAME_PREFIX.length();
        FilePath dir = FilePath.get(dirStr);
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

    public void init() {
        List<Integer> ids = getAllChunkIds();
        if (ids.isEmpty()) {
            currentChunk = new RedoLogChunk(0, config, logSyncService);
        } else {
            int lastId = ids.get(ids.size() - 1);
            for (int id : ids) {
                RedoLogChunk chunk = null;
                try {
                    chunk = new RedoLogChunk(id, config, logSyncService);
                    for (RedoLogRecord r : chunk.readRedoLogRecords()) {
                        r.initPendingRedoLog(pendingRedoLog);
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
    }

    // 第一次打开底层存储的map时调用这个方法，重新执行一次上次已经成功并且在检查点之后的事务操作
    // 有可能多个线程同时调用redo，所以需要加synchronized
    public synchronized void redo(StorageMap<Object, Object> map) {
        List<ByteBuffer> pendingKeyValues = pendingRedoLog.remove(map.getName());
        if (pendingKeyValues != null && !pendingKeyValues.isEmpty()) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = ((TransactionalValueType) map.getValueType()).valueType;
            // 异步redo，忽略操作结果
            AsyncHandler<AsyncResult<Object>> handler = ar -> {
            };
            for (ByteBuffer kv : pendingKeyValues) {
                Object key = kt.read(kv);
                if (kv.get() == 0)
                    map.remove(key, handler);
                else {
                    Object value = vt.read(kv);
                    TransactionalValue tv = TransactionalValue.createCommitted(value);
                    map.put(key, tv, handler);
                }
            }
        }
    }

    void close() {
        currentChunk.close();
    }

    void save() {
        currentChunk.save();
    }

    public void ignoreCheckpoint() {
        currentChunk.ignoreCheckpoint();
    }

    public void setCheckpointService(CheckpointService checkpointService) {
        currentChunk.setCheckpointService(checkpointService);
    }

    public void addFsyncTask(FsyncTask task) {
        currentChunk.addFsyncTask(task);
    }
}
