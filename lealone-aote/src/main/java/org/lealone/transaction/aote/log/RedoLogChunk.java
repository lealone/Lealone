/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.transaction.PendingTransaction;
import org.lealone.transaction.aote.AOTransactionEngine.CheckpointServiceImpl;
import org.lealone.transaction.aote.log.RedoLogRecord.CheckpointRLR;
import org.lealone.transaction.aote.log.RedoLogRecord.PendingCheckpoint;

class RedoLogChunk {

    private static final Logger logger = LoggerFactory.getLogger(RedoLogChunk.class);

    static final String CHUNK_FILE_NAME_PREFIX = "redoLog" + Constants.NAME_SEPARATOR;

    private static String getChunkFileName(Map<String, String> config, int id) {
        String storagePath = config.get(StorageSetting.STORAGE_PATH.name());
        return storagePath + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private static String getArchiveFileName(String archiveDir, int id) {
        return archiveDir + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private static String getArchiveDir(Map<String, String> config) {
        String storagePath = config.get(StorageSetting.STORAGE_PATH.name());
        String archiveDir = storagePath + File.separator
                + MapUtils.getString(config, "archive_dir", "archives");
        if (!FileUtils.exists(archiveDir))
            FileUtils.createDirectories(archiveDir);
        return archiveDir;
    }

    private static final int BUFF_SIZE = 16 * 1024;
    private DataBuffer buff = DataBuffer.create(BUFF_SIZE);

    private DataBuffer checkpointBuff = DataBuffer.create(11); // 1+10,可变long最多需要10个字节
    private FileStorage checkpointChunk;
    private int checkpointChunkId;

    private CheckpointServiceImpl checkpointService;

    private int id;
    private FileStorage fileStorage;
    private final Map<String, String> config;
    private final LogSyncService logSyncService;
    private long pos;

    private final int archiveMaxFiles;
    private final String archiveDir;

    private final long logChunkSize;

    RedoLogChunk(int id, Map<String, String> config, LogSyncService logSyncService) {
        this.id = id;
        this.config = config;
        this.logSyncService = logSyncService;
        fileStorage = getFileStorage(id, config);
        pos = fileStorage.size();

        // 按每小时执行一次checkpoint算，一天24小时，保留3天的归档文件
        archiveMaxFiles = MapUtils.getInt(config, "archive_max_files", 24 * 3);
        archiveDir = getArchiveDir(config);

        logChunkSize = MapUtils.getLong(config, "log_chunk_size", 32 * 1024 * 1024); // 默认32M
    }

    private static FileStorage getFileStorage(int id, Map<String, String> config) {
        String chunkFileName = getChunkFileName(config, id);
        return FileStorage.open(chunkFileName, config);
    }

    // 第一次打开时只有一个线程读，所以用LinkedList即可
    LinkedList<RedoLogRecord> readRedoLogRecords() {
        LinkedList<RedoLogRecord> list = new LinkedList<>();
        if (pos <= 0)
            return list;
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            RedoLogRecord r = RedoLogRecord.read(buffer);
            if (r.isCheckpoint())
                list = new LinkedList<>();// 丢弃之前的
            list.add(r);
        }
        return list;
    }

    void close() {
        save();
        fileStorage.close();
    }

    void save() {
        Scheduler[] waitingSchedulers = logSyncService.getWaitingSchedulers();
        int waitingQueueSize = waitingSchedulers.length;
        AtomicLong logQueueSize = logSyncService.getAsyncLogQueueSize();
        long chunkLength = 0;
        while (logQueueSize.get() > 0) {
            PendingTransaction[] lastPts = new PendingTransaction[waitingQueueSize];
            PendingTransaction[] pts = new PendingTransaction[waitingQueueSize];
            for (int i = 0; i < waitingQueueSize; i++) {
                lastPts[i] = null;
                Scheduler scheduler = waitingSchedulers[i];
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
            PendingCheckpoint pendingCheckpoint = nextPendingCheckpoint(
                    checkpointService.getCheckpoint());
            PendingTransaction pt = nextPendingTransaction(pts);
            while (pt != null || pendingCheckpoint != null) {
                if (pt != null) {
                    if (pendingCheckpoint == null
                            || pt.getLogId() < pendingCheckpoint.getCheckpointId()) {
                        RedoLogRecord r = (RedoLogRecord) pt.getRedoLogRecord();
                        r.write(buff);
                        if (buff.position() > BUFF_SIZE)
                            chunkLength += write(buff);
                        logQueueSize.decrementAndGet();
                        // 提前设置已经同步完成，让调度线程及时回收PendingTransaction
                        if (logSyncService.isPeriodic())
                            pt.setSynced(true);
                        int index = pt.getScheduler().getId();
                        lastPts[index] = pt;
                        pts[index] = pt.getNext();
                        pt = nextPendingTransaction(pts);
                        continue;
                    }

                }
                if (pendingCheckpoint != null) {
                    if (!checkpoint(pendingCheckpoint))
                        chunkLength = 0;
                    logQueueSize.decrementAndGet();
                    pendingCheckpoint = nextPendingCheckpoint(pendingCheckpoint.getNext());
                }
            }
            chunkLength += write(buff);

            if (chunkLength > 0 && !logSyncService.isPeriodic()) {
                chunkLength = 0;
                fileStorage.sync();
            }
            for (int i = 0; i < waitingQueueSize; i++) {
                Scheduler scheduler = waitingSchedulers[i];
                if (scheduler == null || lastPts[i] == null) { // 没有同步过任何RedoLogRecord
                    continue;
                }
                if (!logSyncService.isPeriodic()) {
                    pt = scheduler.getPendingTransaction();
                    while (pt != null) {
                        pt.setSynced(true);
                        if (pt == lastPts[i])
                            break;
                        pt = pt.getNext();
                    }
                }
                scheduler.wakeUp();
            }
            // 避免占用太多内存
            if (buff.capacity() > BUFF_SIZE * 3)
                buff = DataBuffer.create(BUFF_SIZE);

            if (pos > logChunkSize)
                nextChunk(true);
        }
        if (chunkLength > 0 && logSyncService.isPeriodic()) {
            fileStorage.sync();
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

    private PendingCheckpoint nextPendingCheckpoint(PendingCheckpoint pc) {
        while (pc != null) {
            if (pc.isSynced()) {
                pc = pc.getNext();
                continue;
            }
            return pc;
        }
        return null;
    }

    private int write(DataBuffer buff) {
        int length = buff.position();
        if (length > 0) {
            fileStorage.writeFully(pos, buff.getAndFlipBuffer());
            pos += length;
            buff.clear(); // flip后要clear，避免grow时导致OOM问题
        }
        return length;
    }

    // 写满一个RedoLogChunk后不必创建新的RedoLogChunk实例，创建FileStorage实例即可
    // 这样可以有效避免不必要的并发问题
    private void nextChunk(boolean closeFileStorage) {
        if (closeFileStorage)
            fileStorage.close();
        ++id;
        if (id < 0)
            id = 0; // log chunk id用完之后从0开始
        fileStorage = getFileStorage(id, config);
        pos = 0;
    }

    private boolean checkpoint(PendingCheckpoint pendingCheckpoint) {
        CheckpointRLR cp = pendingCheckpoint.getCheckpoint();
        if (cp.isSaved()) {
            cp.write(checkpointBuff);
            checkpointChunk.writeFully(checkpointChunk.size(), checkpointBuff.getAndFlipBuffer());
            checkpointBuff.clear();
            checkpointChunk.sync();
            checkpointChunk.close();
            archiveOldChunkFiles();
            checkpointChunk = null;
            checkpointChunkId = 0;
        } else {
            write(buff);
            checkpointChunk = fileStorage;
            checkpointChunkId = id;
            nextChunk(false);
        }
        pendingCheckpoint.setSynced(true);
        checkpointService.wakeUp();
        return cp.isSaved();
    }

    private void archiveOldChunkFiles() {
        try {
            for (int i = 0; i <= checkpointChunkId; i++) {
                String chunkFileName = getChunkFileName(config, i);
                String archiveFileName = getArchiveFileName(archiveDir, i);
                if (FileUtils.exists(chunkFileName))
                    FileUtils.move(chunkFileName, archiveFileName);
            }
        } catch (Exception e) {
            logger.error("Failed to archive files", e);
        }
        if (archiveMaxFiles > 0)
            deleteOldArchiveFiles();
    }

    private void deleteOldArchiveFiles() {
        try {
            List<Integer> ids = RedoLog.getAllChunkIds(archiveDir);
            if (ids.size() > archiveMaxFiles) {
                for (int i = 0, size = ids.size() - archiveMaxFiles; i < size; i++) {
                    String archiveFileName = getArchiveFileName(archiveDir, ids.get(i));
                    FileUtils.delete(archiveFileName);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to delete files", e);
        }
    }

    void ignoreCheckpoint() {
        if (checkpointChunk != null) {
            checkpointChunk.sync();
            checkpointChunk.close();
            checkpointChunk = null;
            checkpointChunkId = 0;
        }
    }

    void setCheckpointService(CheckpointServiceImpl checkpointService) {
        this.checkpointService = checkpointService;
    }

    CheckpointServiceImpl getCheckpointService() {
        return checkpointService;
    }

    @Override
    public String toString() {
        return "RedoLogChunk[" + fileStorage.getFileName() + "]";
    }
}
