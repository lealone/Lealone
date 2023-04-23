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
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.db.Constants;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.transaction.aote.log.RedoLogRecord.Checkpoint;

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

    private int id;
    private FileStorage fileStorage;
    private final Map<String, String> config;
    private final AtomicInteger logQueueSize = new AtomicInteger(0);
    private final LinkedTransferQueue<RedoLogRecord> logQueue = new LinkedTransferQueue<>();
    private long pos;

    private final int archiveMaxFiles;
    private final String archiveDir;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        this.config = config;
        fileStorage = getFileStorage(id, config);
        pos = fileStorage.size();

        // 按每小时执行一次checkpoint算，一天24小时，保留3天的归档文件
        archiveMaxFiles = MapUtils.getInt(config, "archive_max_files", 24 * 3);
        archiveDir = getArchiveDir(config);
    }

    private static FileStorage getFileStorage(int id, Map<String, String> config) {
        String chunkFileName = getChunkFileName(config, id);
        return FileStorage.open(chunkFileName, config);
    }

    int getId() {
        return id;
    }

    long logChunkSize() {
        return pos;
    }

    int logQueueSize() {
        return logQueueSize.get();
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

    void addRedoLogRecord(RedoLogRecord r) {
        // 虽然这两行不是原子操作，但是也没影响的，最多日志线程空转一下
        logQueue.add(r);
        logQueueSize.incrementAndGet();
    }

    void close() {
        save();
        fileStorage.close();
    }

    void save() {
        if (logQueueSize.get() > 0) {
            int count = 0;
            long chunkLength = 0;
            for (RedoLogRecord r : logQueue) {
                if (r.isCheckpoint()) {
                    if (!checkpoint(r))
                        chunkLength = 0;
                } else {
                    r.write(buff);
                    if (buff.position() > BUFF_SIZE)
                        chunkLength += write(buff);
                }
                count++;
                logQueueSize.decrementAndGet();
            }
            chunkLength += write(buff);
            if (chunkLength > 0) {
                fileStorage.sync();
            }
            for (int i = 0; i < count; i++) {
                RedoLogRecord r = logQueue.poll();
                r.setSynced(true);
                if (!r.isCompleted() && r.getWaitingTransaction() != null) {
                    r.getWaitingTransaction().asyncCommitComplete();
                }
            }
            // 避免占用太多内存
            if (buff.capacity() > BUFF_SIZE * 3)
                buff = DataBuffer.create(BUFF_SIZE);
        }
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

    private boolean checkpoint(RedoLogRecord r) {
        Checkpoint cp = (Checkpoint) r;
        if (cp.isSaved()) {
            r.write(checkpointBuff);
            checkpointChunk.writeFully(checkpointChunk.size(), checkpointBuff.getAndFlipBuffer());
            checkpointBuff.clear();
            checkpointChunk.sync();
            checkpointChunk.close();
            archiveOldChunkFiles();
            checkpointChunk = null;
            checkpointChunkId = 0;
            return true;
        } else {
            write(buff);
            checkpointChunk = fileStorage;
            checkpointChunkId = id;
            fileStorage = getFileStorage(++id, config);
            pos = 0;
            return false;
        }
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

    @Override
    public String toString() {
        return "RedoLogChunk[" + id + ", " + fileStorage.getFileName() + "]";
    }
}
