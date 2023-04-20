/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.util.MapUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

class RedoLogChunk {

    static final String CHUNK_FILE_NAME_PREFIX = "redoLog" + RedoLog.NAME_ID_SEPARATOR;

    private static String getChunkFileName(Map<String, String> config, int id) {
        String storagePath = config.get(StorageSetting.STORAGE_PATH.name());
        return storagePath + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private static String getArchiveFileName(Map<String, String> config, int id) {
        String storagePath = config.get(StorageSetting.STORAGE_PATH.name());
        String archiveDir = storagePath + File.separator
                + MapUtils.getString(config, "archive_dir", "archives");
        if (!FileUtils.exists(archiveDir))
            FileUtils.createDirectories(archiveDir);
        return archiveDir + File.separator + CHUNK_FILE_NAME_PREFIX + id;
    }

    private static final int BUFF_SIZE = 16 * 1024;
    private DataBuffer buff = DataBuffer.create(BUFF_SIZE);

    private int id;
    private FileStorage fileStorage;
    private final Map<String, String> config;
    private final AtomicInteger logQueueSize = new AtomicInteger(0);
    private LinkedTransferQueue<RedoLogRecord> logQueue;
    private long pos;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        this.config = config;
        fileStorage = getFileStorage(id, config);
        logQueue = new LinkedTransferQueue<>();
        pos = fileStorage.size();
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

    int size() {
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
        logQueueSize.incrementAndGet();
        logQueue.add(r);
    }

    void close() {
        save();
        fileStorage.close();
    }

    synchronized void save() {
        if (logQueueSize.get() > 0) {
            LinkedTransferQueue<RedoLogRecord> redoLogRecordQueue = logQueue;
            logQueue = new LinkedTransferQueue<>();
            long chunkLength = 0;
            for (RedoLogRecord r : redoLogRecordQueue) {
                r.write(buff);
                if (r.isCheckpoint()) {
                    write(buff);
                    checkpoint();
                    chunkLength = 0;
                } else {
                    if (buff.position() > BUFF_SIZE)
                        chunkLength += write(buff);
                }
                logQueueSize.decrementAndGet();
            }
            chunkLength += write(buff);
            if (chunkLength > 0) {
                fileStorage.sync();
            }
            for (RedoLogRecord r : redoLogRecordQueue) {
                r.setSynced(true);
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

    private void checkpoint() {
        fileStorage.sync();
        fileStorage.close();
        id++;
        fileStorage = getFileStorage(id, config);
        pos = 0;
        archiveOldChunkFiles();
    }

    private void archiveOldChunkFiles() {
        for (int i = 0; i < id; i++) {
            String chunkFileName = getChunkFileName(config, i);
            String archiveFileName = getArchiveFileName(config, i);
            if (FileUtils.exists(chunkFileName))
                FileUtils.move(chunkFileName, archiveFileName);
        }
    }

    @Override
    public String toString() {
        return "RedoLogChunk[" + id + ", " + fileStorage.getFileName() + "]";
    }
}
