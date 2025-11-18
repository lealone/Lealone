/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.io.File;
import java.nio.ByteBuffer;

import com.lealone.common.compress.CompressDeflate;
import com.lealone.common.compress.CompressLZF;
import com.lealone.common.compress.Compressor;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.DbSetting;
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

    private FileStorage getFileStorage(String chunkFileName) {
        return FileStorage.open(mapBaseDir + File.separator + chunkFileName, map.getConfig());
    }

    public FileStorage getFileStorage(int chunkId) {
        return getFileStorage(chunkManager.getChunkFileName(chunkId));
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
        p.read(buff.slice(), chunkId, offset, pageLength);

        PageInfo pInfo = new PageInfo(p, pos);
        pInfo.buff = buff;
        pInfo.pageLength = pageLength;
        pInfo.setPageLock(ref.getLock());
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

    void save(long dirtyMemory) {
        save(true, false, dirtyMemory);
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
        Chunk lastChunk = chunkManager.getLastChunk();
        boolean isLastChunkUsed = lastChunk != null && !chunkCompactor.isUnusedChunk(lastChunk);
        if (appendModeEnabled && isLastChunkUsed
                && lastChunk.fileStorage.size() + dirtyMemory < maxChunkSize) {
            c = lastChunk;
            appendMode = true;
        } else {
            c = chunkManager.createChunk();
            c.fileStorage = getFileStorage(c.fileName);
        }
        c.mapSize = map.size();
        c.mapMaxKey = map.getMaxKey();

        PageInfo pInfo = map.getRootPageRef().getPageInfo();
        long pos = pInfo.page.write(pInfo, c, chunkBody);
        c.rootPagePos = pos;
        chunkCompactor.clear(); // 提前做一些清理工作，比如删除不再使用的chunk
        c.write(map, chunkBody, appendMode, chunkManager);
        if (!appendMode) {
            chunkManager.addChunk(c);
            chunkManager.setLastChunk(c);
            if (isLastChunkUsed) {
                lastChunk.removeRedoLogAndRemovedPages(map);
            }
        }
    }

    synchronized void writeRedoLog(ByteBuffer log) {
        Chunk c = chunkManager.getLastChunk();
        if (c == null) {
            c = chunkManager.createChunk();
            c.fileStorage = getFileStorage(c.fileName);
            chunkManager.addChunk(c);
            chunkManager.setLastChunk(c);
        }
        c.mapMaxKey = map.getMaxKey();
        c.writeRedoLog(log);
    }

    synchronized ByteBuffer readRedoLog() {
        Chunk c = chunkManager.getLastChunk();
        if (c == null)
            return null;
        else
            return c.readRedoLog();
    }

    synchronized void sync() {
        Chunk c = chunkManager.getLastChunk();
        if (c != null)
            c.fileStorage.sync();
    }
}
