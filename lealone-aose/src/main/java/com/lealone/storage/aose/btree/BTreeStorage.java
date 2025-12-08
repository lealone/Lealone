/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import com.lealone.common.compress.CompressDeflate;
import com.lealone.common.compress.CompressLZF;
import com.lealone.common.compress.Compressor;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.DbSetting;
import com.lealone.storage.FormatVersion;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.aose.btree.chunk.ChunkCompactor;
import com.lealone.storage.aose.btree.chunk.ChunkManager;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageInfo;
import com.lealone.storage.aose.btree.page.PageReference;
import com.lealone.storage.aose.btree.page.PageUtils;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.fs.FileUtils;

/**
 * A persistent storage for btree map.
 */
public class BTreeStorage {

    private final BTreeMap<?, ?> map;
    private final String mapBaseDir;

    private final ChunkManager chunkManager;
    private final ChunkCompactor chunkCompactor;

    private final int pageSize;
    private final int minFillRate;
    private final int maxChunkSize;

    private final BTreeGC bgc;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for high).
     * Even if disabled, the storage may contain (old) compressed pages.
     */
    private final int compressionLevel;
    private Compressor compressorFast;
    private Compressor compressorHigh;

    private boolean closed;

    /**
     * Create and open the storage.
     * 
     * @param map the map to use
     * @throws IllegalStateException if the file is corrupt, or an exception occurred while opening
     */
    BTreeStorage(BTreeMap<?, ?> map) {
        this.map = map;
        pageSize = getIntValue(DbSetting.PAGE_SIZE, Constants.DEFAULT_PAGE_SIZE);
        int minFillRate = getIntValue(StorageSetting.MIN_FILL_RATE, 30);
        if (minFillRate > 50) // 超过50没有实际意义
            minFillRate = 50;
        this.minFillRate = minFillRate;
        compressionLevel = parseCompressionLevel();

        // 32M (32 * 1024 * 1024)，到达一半时就启用GC
        int cacheSize = getIntValue(DbSetting.CACHE_SIZE, Constants.DEFAULT_CACHE_SIZE * 1024 * 1024);
        if (cacheSize > 0 && cacheSize < pageSize)
            cacheSize = pageSize * 2;
        bgc = new BTreeGC(map, cacheSize);

        // 默认256M
        int maxChunkSize = getIntValue(StorageSetting.MAX_CHUNK_SIZE, 256 * 1024 * 1024);
        if (maxChunkSize > Chunk.MAX_SIZE)
            maxChunkSize = Chunk.MAX_SIZE;
        this.maxChunkSize = maxChunkSize;

        chunkManager = new ChunkManager(this);
        chunkCompactor = new ChunkCompactor(this, chunkManager);
        if (map.isInMemory()) {
            mapBaseDir = null;
            return;
        }
        mapBaseDir = map.getStorage().getStoragePath() + File.separator + map.getName();
        if (!FileUtils.exists(mapBaseDir))
            FileUtils.createDirectories(mapBaseDir);
        else {
            chunkManager.init(mapBaseDir);
        }
    }

    private int getIntValue(Enum<?> key, int defaultValue) {
        Object value = map.getConfig(key.name());
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value != null) {
            String str = value.toString().trim().toLowerCase();
            if (str.endsWith("k")) {
                str = str.substring(0, str.length() - 1).trim();
                return Integer.parseInt(str) * 1024;
            } else if (str.endsWith("m")) {
                str = str.substring(0, str.length() - 1).trim();
                return Integer.parseInt(str) * 1024 * 1024;
            } else {
                return Integer.parseInt(str);
            }
        } else {
            return defaultValue;
        }
    }

    private int parseCompressionLevel() {
        Object value = map.getConfig(DbSetting.COMPRESS.name());
        if (value == null)
            return Compressor.NO;
        else {
            String str = value.toString().trim().toUpperCase();
            if (str.equals("NO"))
                return Compressor.NO;
            else if (str.equals("LZF"))
                return Compressor.LZF;
            else if (str.equals("DEFLATE"))
                return Compressor.DEFLATE;
            else
                return Integer.parseInt(str);
        }
    }

    public BTreeMap<?, ?> getMap() {
        return map;
    }

    public ChunkManager getChunkManager() {
        return chunkManager;
    }

    public ChunkCompactor getChunkCompactor() {
        return chunkCompactor;
    }

    public BTreeGC getBTreeGC() {
        return bgc;
    }

    public int getCompressionLevel() {
        return compressionLevel;
    }

    public Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    public Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getMinFillRate() {
        return minFillRate;
    }

    public long getDiskSpaceUsed() {
        return FileUtils.folderSize(new File(mapBaseDir));
    }

    public long getMemorySpaceUsed() {
        return bgc.getUsedMemory();
    }

    public FileStorage getFileStorage(String chunkFileName) {
        return FileStorage.open(mapBaseDir + File.separator + chunkFileName, map.getConfig());
    }

    public IllegalStateException panic(int errorCode, String message, Object... arguments) {
        IllegalStateException e = DataUtils.newIllegalStateException(errorCode, message, arguments);
        return panic(e);
    }

    public IllegalStateException panic(IllegalStateException e) {
        closeImmediately(true);
        return e;
    }

    public ByteBuffer readPageBuffer(long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Chunk c = chunkManager.getChunk(pos);
        long filePos = Chunk.getFilePos(PageUtils.getPageOffset(pos));
        int pageLength = c.getPageLength(pos);
        if (pageLength < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1} ", pageLength, filePos);
        }
        return c.fileStorage.readFully(filePos, pageLength);
    }

    public PageInfo readPage(PageReference ref, long pos) {
        ByteBuffer pageBuff = readPageBuffer(pos);
        int pageLength = pageBuff.limit();
        return readPage(ref, pos, pageBuff, pageLength);
    }

    public PageInfo readPage(PageReference ref, long pos, ByteBuffer buff, int pageLength) {
        DataUtils.checkNotNull(ref, "ref");
        int type = PageUtils.getPageType(pos);
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        Page p = Page.create(map, type, buff);
        p.setRef(ref);
        // buff要复用，并且要支持多线程同时读，所以直接用slice
        int metaVersion = p.read(buff.slice(), chunkId, offset, pageLength);

        PageInfo pInfo = new PageInfo(p, pos);
        pInfo.buff = buff;
        pInfo.pageLength = pageLength;
        pInfo.setPageLock(ref.getLock());
        pInfo.metaVersion = metaVersion;
        return pInfo;
    }

    synchronized void clear() {
        if (map.isInMemory())
            return;
        bgc.close();
        chunkManager.close();
    }

    synchronized void remove() {
        closeImmediately(false);
        if (map.isInMemory())
            return;
        FileUtils.deleteRecursive(mapBaseDir, true);
    }

    boolean isClosed() {
        return closed;
    }

    /**
     * Close the file and the storage. Unsaved changes are written to disk first.
     */
    void close() {
        closeStorage(false);
    }

    /**
     * Close the file and the storage, without writing anything.
     */
    private void closeImmediately(boolean ignoreError) {
        try {
            closeStorage(true);
        } catch (Exception e) {
            if (!ignoreError) {
                throw DbException.convert(e);
            }
        }
    }

    private synchronized void closeStorage(boolean immediate) {
        if (closed)
            return;
        try {
            if (!immediate)
                save();
            chunkManager.close();
            closed = true;
        } finally {
            bgc.close();
        }
    }

    void save() {
        save(true, false, map.collectDirtyMemory());
    }

    /**
     * Save all changes and persist them to disk.
     * This method does nothing if there are no unsaved changes.
     */
    synchronized void save(boolean compact, boolean appendModeEnabled, long dirtyMemory) {
        if (!map.hasUnsavedChanges() || closed || map.isInMemory()) {
            return;
        }
        if (map.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "This storage is read-only");
        }
        try {
            if (compact)
                chunkCompactor.executeCompact();
            executeSave(appendModeEnabled, dirtyMemory);
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    private void executeSave(boolean appendModeEnabled, long dirtyMemory) {
        DataBuffer chunkBody = DataBuffer.createDirect((int) dirtyMemory);
        boolean appendMode = false;
        Chunk c;
        Chunk lastChunk;
        String lastUnusedChunk = null;
        long lastRedoLogPos = -1;
        boolean isLastChunkUsed = false;
        redoLogLock.lock();
        try {
            lastChunk = chunkManager.getLastChunk();
            if (lastChunk != null) {
                lastRedoLogPos = lastChunk.size();
                // 老版本的chunk不再append
                if (appendModeEnabled && FormatVersion.isOldFormatVersion(lastChunk.formatVersion)) {
                    appendModeEnabled = false;
                }
                isLastChunkUsed = !chunkCompactor.isUnusedChunk(lastChunk);
                if (!isLastChunkUsed || lastChunk.isOnlyRedoLog())
                    lastUnusedChunk = lastChunk.fileName;
            }
            if (appendModeEnabled && isLastChunkUsed && lastChunk.size() + dirtyMemory < maxChunkSize) {
                c = lastChunk;
                appendMode = true;
            } else {
                c = chunkManager.createChunk();
                c.fileStorage = getFileStorage(c.fileName);
            }
        } finally {
            redoLogLock.unlock();
        }
        c.mapSize = map.size();
        c.mapMaxKey = map.getMaxKey();

        PageInfo pInfo = map.getRootPageRef().getPageInfo();
        long pos = pInfo.page.write(pInfo, c, chunkBody, new AtomicBoolean(false));
        c.rootPagePos = pos;

        // 提前清理UnusedChunks中的pages，这样在RemovedPages中不会保留它们，也不会写到最新的chunk中
        chunkCompactor.clearUnusedChunkPages();

        c.setLastTransactionId(map.getLastTransactionId());
        c.setLastRedoLogPos(lastRedoLogPos);
        c.setLastUnusedChunk(lastUnusedChunk);

        c.write(chunkBody, chunkManager, appendMode);

        // 最新的chunk写成功后再删除UnusedChunks，不能提前删除，因为UnusedChunks也包含被重写的chunk
        // 若最新的chunk写失败了，被重写的chunk文件也提前删除就会丢失数据
        chunkCompactor.removeUnusedChunks();

        if (!appendMode) {
            redoLogLock.lock();
            try {
                chunkManager.addChunk(c);
                chunkManager.setLastChunk(c);
            } finally {
                redoLogLock.unlock();
            }
            if (lastChunk != null) {
                // 提前删除
                if (lastUnusedChunk != null && lastRedoLogPos == lastChunk.size()) {
                    chunkManager.removeUnusedChunk(lastChunk);
                }

                if (lastChunk.getLastUnusedChunk() != null) {
                    Chunk chunk = chunkManager.findChunk(lastChunk.getLastUnusedChunk());
                    // 如果已经提前删除那什么都不需要做
                    if (chunk != null)
                        chunkManager.removeUnusedChunk(chunk);
                } else {
                    // 倒数第三个chunk的redo log可以删除了
                    Chunk thirdLastChunk = chunkManager.findThirdLastChunk();
                    if (thirdLastChunk != null) {
                        thirdLastChunk.removeRedoLogAndRemovedPages(map);
                    }
                }
            }
        }
    }

    private final ReentrantLock redoLogLock = new ReentrantLock();
    private Chunk lastWriteChunk;

    void writeRedoLog(ByteBuffer log) {
        redoLogLock.lock();
        try {
            Chunk c = chunkManager.getLastChunk();
            if (c == null) {
                c = chunkManager.createChunk();
                c.fileStorage = getFileStorage(c.fileName);
                chunkManager.addChunk(c);
                chunkManager.setLastChunk(c);
            }
            c.mapMaxKey = map.getMaxKey();
            c.writeRedoLog(log);

            // 写完后有可能另一个线程刷脏页会创建新的chunk，所以调用sync时不能直接用chunkManager.getLastChunk()
            lastWriteChunk = c;
        } finally {
            redoLogLock.unlock();
        }
    }

    ByteBuffer readRedoLog() {
        redoLogLock.lock();
        try {
            Chunk c = chunkManager.getLastChunk();
            if (c == null) {
                return null;
            } else {
                int size1 = c.getRedoLogSize();
                ByteBuffer buffer = null;
                // 倒数第二个chunk也可能有RedoLog
                Long seq = ChunkManager.getSeq(c.fileName);
                if (seq > 1 && c.getLastRedoLogPos() > 0) {
                    Chunk secondLastChunk = chunkManager.findChunk(seq - 1);
                    if (secondLastChunk != null) {
                        int size2 = (int) (secondLastChunk.size() - c.getLastRedoLogPos());
                        int capacity = size1 + size2;
                        buffer = ByteBuffer.allocate(capacity);
                        buffer.limit(size2);
                        secondLastChunk.fileStorage.readFully(c.getLastRedoLogPos(), size2, buffer);
                        buffer.position(size2);
                        buffer.limit(capacity);
                    }
                }
                if (size1 <= 0) {
                    return buffer;
                } else {
                    if (buffer == null)
                        buffer = ByteBuffer.allocate(size1);
                    c.fileStorage.readFully(c.getRedoLogPos(), size1, buffer);
                    return buffer;
                }
            }
        } finally {
            redoLogLock.unlock();
        }
    }

    void sync() {
        redoLogLock.lock();
        try {
            if (lastWriteChunk != null) {
                lastWriteChunk.fileStorage.sync();
                lastWriteChunk = null;
            } else {
                Chunk c = chunkManager.getLastChunk();
                if (c != null)
                    c.fileStorage.sync();
            }
        } finally {
            redoLogLock.unlock();
        }
    }

    boolean validateRedoLog(long lastTransactionId) {
        redoLogLock.lock();
        try {
            Chunk c = chunkManager.getLastChunk();
            if (c != null && c.getLastTransactionId() >= lastTransactionId)
                return true;
            ByteBuffer log = readRedoLog();
            if (log == null)
                return false;
            while (log.hasRemaining()) {
                int len = log.getInt();
                int pos = log.position();
                int type = log.get();
                if (type > 1) {
                    long transactionId = DataUtils.readVarLong(log);
                    if (transactionId == lastTransactionId)
                        return true;
                }
                log.position(pos + len);
            }
            return false;
        } finally {
            redoLogLock.unlock();
        }
    }
}
