/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.log;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

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

    private static final int BUFF_SIZE = 16 * 1024;
    private DataBuffer buff = DataBuffer.create(BUFF_SIZE);

    private final int id;
    private final FileStorage fileStorage;
    private final Map<String, String> config;
    private final AtomicInteger logQueueSize = new AtomicInteger(0);
    private LinkedTransferQueue<RedoLogRecord> logQueue;
    private long pos;

    RedoLogChunk(int id, Map<String, String> config) {
        this.id = id;
        this.config = config;
        String chunkFileName = getChunkFileName(config, id);
        fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, config);
        logQueue = new LinkedTransferQueue<>();
        pos = fileStorage.size();
        if (pos > 0)
            read();
    }

    private void read() {
        ByteBuffer buffer = fileStorage.readFully(0, (int) pos);
        while (buffer.remaining() > 0) {
            RedoLogRecord r = RedoLogRecord.read(buffer);
            if (r.isCheckpoint())
                logQueue = new LinkedTransferQueue<>(); // 丢弃之前的
            logQueue.add(r);
        }
    }

    int getId() {
        return id;
    }

    int size() {
        return logQueueSize.get();
    }

    void addRedoLogRecord(RedoLogRecord r) {
        // 虽然这两行不是原子操作，但是也没影响的，最多日志线程空转一下
        logQueueSize.incrementAndGet();
        logQueue.add(r);
    }

    LinkedTransferQueue<RedoLogRecord> getAndResetRedoLogRecords() {
        LinkedTransferQueue<RedoLogRecord> oldQueue = logQueue;
        logQueue = new LinkedTransferQueue<>();
        return oldQueue;
    }

    void close() {
        save();
        fileStorage.close();
    }

    synchronized void save() {
        if (logQueueSize.get() > 0) {
            LinkedTransferQueue<RedoLogRecord> redoLogRecordQueue = getAndResetRedoLogRecords();
            long chunkLength = 0;
            for (RedoLogRecord r : redoLogRecordQueue) {
                if (r.isCheckpoint()) {
                    deleteOldChunkFiles();
                    fileStorage.truncate(0);
                    buff.clear();
                    pos = 0;
                }
                r.write(buff);
                if (buff.position() > BUFF_SIZE)
                    chunkLength += write(buff);
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
