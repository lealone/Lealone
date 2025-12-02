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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.lealone.common.util.DataUtils;
import com.lealone.common.util.MapUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.link.LinkableList;
import com.lealone.db.lock.Lockable;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.value.ValueString;
import com.lealone.storage.FormatVersion;
import com.lealone.storage.StorageMap;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.fs.FileUtils;
import com.lealone.storage.type.StorageDataType;
import com.lealone.transaction.PendingTransaction;
import com.lealone.transaction.aote.TransactionalValue;

public class RedoLog {

    private static final int BUFF_SIZE = 16 * 1024;

    private final Map<String, String> config;
    private final LogSyncService logSyncService;

    // key: mapName, value: map key/value ByteBuffer list
    private HashMap<String, List<ByteBuffer>> pendingRedoLog;

    // 保存需要写redo log的StorageMap，索引或内存表对应的StorageMap不需要写redo log
    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    // 如果事务涉及多个表，要等所有事务的redo log都fsync后才能执行检查点刷脏页
    private final LinkableList<PendingTransaction> pendingTransactions = new LinkableList<>();
    private long lastTransactionId;

    public RedoLog(Map<String, String> config, LogSyncService logSyncService) {
        this.config = config;
        this.logSyncService = logSyncService;
    }

    public void addMap(StorageMap<?, ?> map) {
        maps.put(map.getName(), map);
    }

    public void removeMap(String mapName) {
        maps.remove(mapName);
    }

    public long getLastTransactionId() {
        return lastTransactionId;
    }

    // 兼容老版本的redo log
    public void init() {
        String logDir = getLogDir();
        if (!FileUtils.exists(logDir))
            return;
        List<Integer> ids = getAllChunkIds(logDir);
        if (!ids.isEmpty()) {
            pendingRedoLog = new HashMap<>();
            String namePrefix = logDir + File.separator + "redoLog" + Constants.NAME_SEPARATOR;
            // 第一次打开时只有一个线程读，所以用LinkedList即可
            LinkedList<RedoLogRecord> redoLogRecords = new LinkedList<>();
            for (int id : ids) {
                FileStorage fileStorage = FileStorage.open(namePrefix + id, config);
                try {
                    int pos = (int) fileStorage.size();
                    if (pos > 0) {
                        ByteBuffer buffer = fileStorage.readFully(0, pos);
                        while (buffer.remaining() > 0) {
                            RedoLogRecord r = RedoLogRecord.read(buffer);
                            // 遇到检查点可以丢弃前面的redo log
                            if (r.isCheckpoint())
                                redoLogRecords = new LinkedList<>();
                            else
                                redoLogRecords.add(r);
                        }
                    }
                } finally {
                    fileStorage.close();
                }
            }
            for (RedoLogRecord r : redoLogRecords) {
                r.initPendingRedoLog(pendingRedoLog);
            }
        }
    }

    private String getLogDir() {
        return config.get("base_dir") + File.separator
                + MapUtils.getString(config, "redo_log_dir", "redo_log");
    }

    private List<Integer> getAllChunkIds(String logDir) {
        ArrayList<Integer> ids = new ArrayList<>();
        String namePrefix = "redoLog" + Constants.NAME_SEPARATOR;
        int prefixLength = namePrefix.length();
        FilePath dir = FilePath.get(logDir);
        for (FilePath fp : dir.newDirectoryStream()) {
            String fullName = fp.getName();
            if (fullName.startsWith(namePrefix)) {
                int id = Integer.parseInt(fullName.substring(prefixLength));
                ids.add(id);
            }
        }
        Collections.sort(ids); // 必须排序，按id从小到大的顺序读取文件，才能正确的redo
        return ids;
    }

    // 重新执行一次上次已经成功并且在检查点之后的事务操作
    @SuppressWarnings("unchecked")
    public void redo(StorageMap<?, ?> map0, List<StorageMap<?, ?>> indexMaps0) {
        // java的泛型很烂，这里做一下强制转换，否则后续的代码有编译错误
        final StorageMap<Object, Object> map = (StorageMap<Object, Object>) map0;
        final List<StorageMap<Object, Object>> indexMaps;

        if (indexMaps0 != null) {
            indexMaps = new ArrayList<>(indexMaps0.size());
            for (StorageMap<?, ?> im : indexMaps0) {
                indexMaps.add((StorageMap<Object, Object>) im);
            }
        } else {
            indexMaps = null;
        }

        StorageDataType kt = map.getKeyType();
        StorageDataType vt = map.getValueType().getRawType();
        // 异步redo，忽略操作结果
        AsyncResultHandler<Object> handler = AsyncResultHandler.emptyHandler();

        // lealone 6.1.0之前的版本若是存在全局redo log，先执行它
        if (pendingRedoLog != null) {
            List<ByteBuffer> pendingKeyValues;
            // 多个线程打开不同数据库时会同时调用redo，所以需要加synchronized
            synchronized (pendingRedoLog) {
                pendingKeyValues = pendingRedoLog.remove(map.getName());
                if (indexMaps != null) {
                    // lealone 6.1.0之前的版本对index修改时也写redo log，现在可以直接忽略了
                    for (StorageMap<?, ?> im : indexMaps) {
                        pendingRedoLog.remove(im.getName());
                    }
                }
            }
            if (pendingKeyValues != null && !pendingKeyValues.isEmpty()) {
                for (ByteBuffer kv : pendingKeyValues) {
                    redo(map, indexMaps, kt, vt, kv, handler, FormatVersion.FORMAT_VERSION_1);
                }
                map.save();
            }
            // 当所有旧版本的全局redo log都执行完后就可以直接删除了
            synchronized (pendingRedoLog) {
                if (pendingRedoLog.isEmpty()) {
                    FileUtils.deleteRecursive(getLogDir(), true);
                }
            }
        }

        ByteBuffer log = map.readRedoLog();
        if (log != null) {
            while (log.hasRemaining()) {
                redo(map, indexMaps, kt, vt, log, handler, FormatVersion.FORMAT_VERSION);
            }
        }
    }

    private void redo(StorageMap<Object, Object> map, List<StorageMap<Object, Object>> indexMaps,
            StorageDataType kt, StorageDataType vt, ByteBuffer kv, AsyncResultHandler<Object> handler,
            int formatVersion) {
        Object key;
        byte type;
        int metaVersion = 0;
        if (FormatVersion.isOldFormatVersion(formatVersion)) {
            key = kt.read(kv, formatVersion);
            type = kv.get();
            if (type != 0)
                metaVersion = DataUtils.readVarInt(kv); // 老版本也写了version，提前读出来
        } else {
            int len = kv.getInt();
            int pos = kv.position();
            type = kv.get();
            if (type == 1) {
                metaVersion = DataUtils.readVarInt(kv);
            } else if (type > 1) {
                long transactionId = DataUtils.readVarLong(kv);
                if (!readMapNames(map, kv, transactionId)) {
                    kv.position(pos + len); // 无效的事务直接忽略
                    return;
                }
                if (type == 3) {
                    metaVersion = DataUtils.readVarInt(kv);
                }
            }
            key = kt.read(kv, formatVersion);
        }
        if (type == 0 || type == 2) {
            map.remove(key, ar -> {
                Object result = ar.getResult();
                if (result != null) {
                    Object value = ((Lockable) result).getValue();
                    if (indexMaps != null) {
                        for (StorageMap<Object, Object> im : indexMaps) {
                            StorageDataType ikt = im.getKeyType();
                            Object indexKey = ikt.convertToIndexKey(key, value);
                            im.remove(indexKey, handler);
                        }
                    }
                }
            });
        } else {
            Object value = vt.read(kv, FormatVersion.FORMAT_VERSION); // 新老版本的redo log都用新的格式读
            Lockable lockable;
            if (value instanceof Lockable) {
                lockable = (Lockable) value;
                lockable.setKey(key);
            } else {
                lockable = TransactionalValue.createCommitted(value);
            }
            if (vt.supportsRedo()) {
                vt.redo(lockable, metaVersion);
            } else {
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

    private boolean readMapNames(StorageMap<Object, Object> map, ByteBuffer buff, long transactionId) {
        int size = DataUtils.readVarInt(buff);
        for (int i = 0; i < size; i++) {
            String mapName = ValueString.type.read(buff);
            if (!map.getName().equalsIgnoreCase(mapName)) {
                if (!map.getStorage().getMap(mapName).validateRedoLog(transactionId))
                    return false;
            }
        }
        return true;
    }

    public void save() {
        // 事务中涉及的StorageMap
        HashMap<StorageMap<Object, ?>, DataBuffer> logs = new HashMap<>();

        InternalScheduler[] waitingSchedulers = logSyncService.getWaitingSchedulers();
        int waitingSchedulerCount = waitingSchedulers.length;
        AtomicLong logQueueSize = logSyncService.getAsyncLogQueueSize();
        long logLength = 0;
        while (logQueueSize.get() > 0) {
            PendingTransaction[] lastPts = new PendingTransaction[waitingSchedulerCount];
            PendingTransaction[] pts = new PendingTransaction[waitingSchedulerCount];
            // 先找到每个调度器还没有同步的PendingTransaction
            for (int i = 0; i < waitingSchedulerCount; i++) {
                InternalScheduler scheduler = waitingSchedulers[i];
                if (scheduler == null) {
                    continue;
                }
                PendingTransaction pt = scheduler.getPendingTransaction();
                while (pt != null) {
                    if (pt.isSynced()) {
                        pt = pt.getNext();
                        continue;
                    }
                    pts[scheduler.getId()] = pt;
                    break;
                }
            }
            int buffLength = 0;
            // 找出提交时间戳最小的PendingTransaction
            PendingTransaction pt = nextPendingTransaction(pts);
            if (pt == null && logQueueSize.get() > 0) {
                logQueueSize.decrementAndGet();
            }
            while (pt != null) {
                RedoLogRecord r = (RedoLogRecord) pt.getRedoLogRecord();
                Set<Integer> serviceIndexs = r.getRedoLogServiceIndexs();
                if (serviceIndexs == null
                        || !serviceIndexs.contains(logSyncService.getSyncServiceIndex())) {
                    int index = pt.getScheduler().getId();
                    pts[index] = pt.getNext();
                    pt = nextPendingTransaction(pts);
                    continue;
                }
                pt.setMaps(r.getMaps());
                if (serviceIndexs.size() > 1) {
                    pendingTransactions.add(pt);
                }
                lastTransactionId = pt.getTransaction().getTransactionId();

                buffLength += r.write(logs, maps);
                if (buffLength > BUFF_SIZE) {
                    buffLength = 0;
                    logLength += write(logs);
                }
                logQueueSize.decrementAndGet();
                // 提前设置已经同步完成，让调度线程及时回收PendingTransaction
                if (logSyncService.isPeriodic()) {
                    setSynced(pt);
                }
                int index = pt.getScheduler().getId();
                lastPts[index] = pt;
                pts[index] = pt.getNext();
                pt = nextPendingTransaction(pts);
            }

            if (buffLength > 0)
                logLength += write(logs);

            if (logLength > 0 && !logSyncService.isPeriodic()) {
                logLength = 0;
                sync(logs);
            }
            for (int i = 0; i < waitingSchedulerCount; i++) {
                InternalScheduler scheduler = waitingSchedulers[i];
                if (scheduler == null || lastPts[i] == null) { // 没有同步过任何RedoLogRecord
                    continue;
                }
                if (!logSyncService.isPeriodic()) {
                    pt = scheduler.getPendingTransaction();
                    while (pt != null) {
                        setSynced(pt);
                        if (pt == lastPts[i])
                            break;
                        pt = pt.getNext();
                    }
                }
                scheduler.wakeUp();
            }
        }
        if (logLength > 0 && logSyncService.isPeriodic()) {
            sync(logs);
        }
    }

    private void setSynced(PendingTransaction pt) {
        if (pt.isSynced())
            return;
        ConcurrentHashMap<StorageMap<?, ?>, AtomicBoolean> ptMaps = pt.getMaps();
        if (ptMaps != null) {
            for (Entry<StorageMap<?, ?>, AtomicBoolean> e : ptMaps.entrySet()) {
                if (maps.containsKey(e.getKey().getName())) {
                    e.getValue().set(true);
                }
            }
            boolean isAllSynced = true;
            for (Entry<StorageMap<?, ?>, AtomicBoolean> e : ptMaps.entrySet()) {
                if (e.getValue().get() == false) {
                    isAllSynced = false;
                    break;
                }
            }
            if (isAllSynced)
                pt.setSynced(true);
        } else {
            pt.setSynced(true);
        }
    }

    private PendingTransaction nextPendingTransaction(PendingTransaction[] pts) {
        PendingTransaction minPendingTransaction = null;
        long minCommitTimestamp = Long.MAX_VALUE;
        for (int i = 0, len = pts.length; i < len; i++) {
            PendingTransaction pt = pts[i];
            while (pt != null) {
                if (pt.isSynced()) {
                    pt = pt.getNext();
                    pts[i] = pt;
                    continue;
                }
                if (pt.getLogId() < minCommitTimestamp) {
                    minCommitTimestamp = pt.getLogId();
                    minPendingTransaction = pt;
                }
                break;
            }
        }
        return minPendingTransaction;
    }

    private int write(HashMap<StorageMap<Object, ?>, DataBuffer> logs) {
        int length = 0;
        for (Entry<StorageMap<Object, ?>, DataBuffer> e : logs.entrySet()) {
            DataBuffer log = e.getValue();
            ByteBuffer buffer = log.getAndFlipBuffer();
            length += buffer.limit();
            e.getKey().writeRedoLog(buffer);
            log.clear();
        }
        return length;
    }

    private void sync(HashMap<StorageMap<Object, ?>, DataBuffer> logs) {
        for (StorageMap<Object, ?> m : logs.keySet()) {
            m.sync();
        }
        runPendingTransactions();
    }

    public void runPendingTransactions() {
        if (pendingTransactions.isEmpty())
            return;
        PendingTransaction pt = pendingTransactions.getHead();
        while (pt != null && pt.isSynced()) {
            RedoLogRecord r = (RedoLogRecord) pt.getRedoLogRecord();
            r.removeRedoLogServiceIndex(logSyncService.getSyncServiceIndex());
            if (r.getRedoLogServiceIndexs() == null || r.getRedoLogServiceIndexs().isEmpty()) {
                pt = pt.getNext();
                pendingTransactions.decrementSize();
                pendingTransactions.setHead(pt);
            } else {
                break;
            }
        }
        if (pendingTransactions.getHead() == null) {
            pendingTransactions.setTail(null);
        }
    }

    public boolean hasPendingTransactions() {
        return !pendingTransactions.isEmpty();
    }
}
