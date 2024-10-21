/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lealone.common.util.MapUtils;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.lock.Lockable;
import com.lealone.storage.StorageMap;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileUtils;
import com.lealone.storage.type.StorageDataType;
import com.lealone.transaction.aote.CheckpointService;
import com.lealone.transaction.aote.CheckpointService.FsyncTask;
import com.lealone.transaction.aote.TransactionalValue;

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

    // 重新执行一次上次已经成功并且在检查点之后的事务操作
    @SuppressWarnings("unchecked")
    public void redo(StorageMap<?, ?> map0, List<StorageMap<?, ?>> indexMaps0) {
        // java的泛型很烂，这里做一下强制转换，否则后续的代码有编译错误
        final StorageMap<Object, Object> map = (StorageMap<Object, Object>) map0;
        final List<StorageMap<Object, Object>> indexMaps;
        final List<ByteBuffer> pendingKeyValues;
        // 多个线程打开不同数据库时会同时调用redo，所以需要加synchronized
        synchronized (pendingRedoLog) {
            pendingKeyValues = pendingRedoLog.remove(map.getName());
            if (indexMaps0 != null) {
                indexMaps = new ArrayList<>(indexMaps0.size());
                // <=lealone 6.0.1的版本对index修改时也写redo log，现在可以直接忽略了
                for (StorageMap<?, ?> im : indexMaps0) {
                    pendingRedoLog.remove(im.getName());
                    indexMaps.add((StorageMap<Object, Object>) im);
                }
            } else {
                indexMaps = null;
            }
        }
        if (pendingKeyValues != null && !pendingKeyValues.isEmpty()) {
            StorageDataType kt = map.getKeyType();
            StorageDataType vt = map.getValueType().getRawType();
            // 异步redo，忽略操作结果
            AsyncHandler<AsyncResult<Object>> handler = ar -> {
            };
            for (ByteBuffer kv : pendingKeyValues) {
                Object key = kt.read(kv);
                if (kv.get() == 0) {
                    map.remove(key, ar -> {
                        Object value = ((Lockable) ar.getResult()).getValue();
                        if (indexMaps != null) {
                            for (StorageMap<Object, Object> im : indexMaps) {
                                StorageDataType ikt = im.getKeyType();
                                Object indexKey = ikt.convertToIndexKey(key, value);
                                im.remove(indexKey);
                            }
                        }
                    });
                } else {
                    Object value = vt.read(kv);
                    Lockable lockable;
                    if (value instanceof Lockable) {
                        lockable = (Lockable) value;
                        lockable.setKey(key);
                    } else {
                        lockable = TransactionalValue.createCommitted(value);
                    }
                    map.put(key, lockable, handler);
                    if (indexMaps != null) {
                        for (StorageMap<Object, Object> im : indexMaps) {
                            StorageDataType ikt = im.getKeyType();
                            Object indexKey = ikt.convertToIndexKey(key, value);
                            im.put(indexKey, indexKey, handler);
                        }
                    }
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
