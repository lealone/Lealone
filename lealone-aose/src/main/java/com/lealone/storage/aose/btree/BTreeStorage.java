/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.lealone.common.compress.CompressDeflate;
import com.lealone.common.compress.CompressLZF;
import com.lealone.common.compress.Compressor;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.Constants;
import com.lealone.db.DataBuffer;
import com.lealone.db.DbSetting;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.storage.StorageSetting;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.aose.btree.chunk.ChunkCompactor;
import com.lealone.storage.aose.btree.chunk.ChunkManager;
import com.lealone.storage.aose.btree.page.Page;
import com.lealone.storage.aose.btree.page.PageInfo;
import com.lealone.storage.aose.btree.page.PageUtils;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileStorage;
import com.lealone.storage.fs.FileUtils;
import com.lealone.transaction.TransactionEngine;

/**
 * A persistent storage for btree map.
 */
public class BTreeStorage {

    private final BTreeMap<?, ?> map;
    private final String mapBaseDir;

    private final ChunkManager chunkManager;

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
        pageSize = getIntValue(DbSetting.PAGE_SIZE.name(), Constants.DEFAULT_PAGE_SIZE);
        int minFillRate = getIntValue(StorageSetting.MIN_FILL_RATE.name(), 30);
        if (minFillRate > 50) // 超过50没有实际意义
            minFillRate = 50;
        this.minFillRate = minFillRate;
        compressionLevel = parseCompressionLevel();

        // 32M (32 * 1024 * 1024)，到达一半时就启用GC
        int cacheSize = getIntValue(DbSetting.CACHE_SIZE.name(),
                Constants.DEFAULT_CACHE_SIZE * 1024 * 1024);
        if (cacheSize > 0 && cacheSize < pageSize)
            cacheSize = pageSize * 2;
        bgc = new BTreeGC(map, cacheSize);

        // 默认256M
        int maxChunkSize = getIntValue(StorageSetting.MAX_CHUNK_SIZE.name(), 256 * 1024 * 1024);
        if (maxChunkSize > Chunk.MAX_SIZE)
            maxChunkSize = Chunk.MAX_SIZE;
        this.maxChunkSize = maxChunkSize;

        chunkManager = new ChunkManager(this);
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

    private int getIntValue(String key, int defaultValue) {
        Object value = map.getConfig(key);
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

    public IllegalStateException panic(int errorCode, String message, Object... arguments) {
        IllegalStateException e = DataUtils.newIllegalStateException(errorCode, message, arguments);
        return panic(e);
    }

    public IllegalStateException panic(IllegalStateException e) {
        closeImmediately(true);
        return e;
    }

    public ChunkManager getChunkManager() {
        return chunkManager;
    }

    public SchedulerFactory getSchedulerFactory() {
        return map.getSchedulerFactory();
    }

    // ChunkCompactor在重写chunk中的page时会用到
    public void markDirtyLeafPage(long pos) {
        Page p = map.getRootPage();
        // 刚保存过然后接着被重写
        if (p.getPos() == pos) {
            p.markDirty();
            return;
        }
        Page leaf = readPage(pos).page;
        Object key = leaf.getKey(0);
        map.markDirty(key);
    }

    public PageInfo readPage(long pos) {
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
        ByteBuffer buff = c.fileStorage.readFully(filePos, pageLength);
        return readPage(pos, buff, pageLength);
    }

    public PageInfo readPage(long pos, ByteBuffer buff, int pageLength) {
        int type = PageUtils.getPageType(pos);
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        Page p = Page.create(map, type);
        // buff要复用，并且要支持多线程同时读，所以直接用slice
        p.read(buff.slice(), chunkId, offset, pageLength);

        PageInfo pInfo = new PageInfo(p, pos);
        if (!p.isNode() || BTreeGC.gcNodePages) {
            pInfo.buff = buff;
            pInfo.pageLength = pageLength;
        }
        return pInfo;
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

    /**
     * Get the maximum cache size, in MB.
     * 
     * @return the cache size
     */
    public long getCacheSize() {
        return bgc.getMaxMemory();
    }

    /**
     * Set the read cache size in MB.
     * 
     * @param mb the cache size in MB.
     */
    public void setCacheSize(long mb) {
        bgc.setMaxMemory(mb * 1024 * 1024);
    }

    public long getDiskSpaceUsed() {
        return FileUtils.folderSize(new File(mapBaseDir));
    }

    public long getMemorySpaceUsed() {
        return bgc.getUsedMemory();
    }

    synchronized void clear() {
        if (map.isInMemory())
            return;
        bgc.close();
        chunkManager.close();
    }

    /**
     * Remove this storage.
     */
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

    private int collectDirtyMemory(TransactionEngine te) {
        if (te == null)
            te = TransactionEngine.getDefaultTransactionEngine();
        return (int) map.collectDirtyMemory(te, null);
    }

    void save() {
        save(true, collectDirtyMemory(null));
    }

    void save(int dirtyMemory) {
        save(true, dirtyMemory);
    }

    /**
     * Save all changes and persist them to disk.
     * This method does nothing if there are no unsaved changes.
     */
    synchronized void save(boolean compact, int dirtyMemory) {
        if (!map.hasUnsavedChanges() || closed || map.isInMemory()) {
            return;
        }
        if (map.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "This storage is read-only");
        }
        try {
            executeSave(true, dirtyMemory);
            if (compact)
                new ChunkCompactor(this, chunkManager).executeCompact();
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    public synchronized void executeSave(boolean appendModeEnabled) {
        executeSave(appendModeEnabled, collectDirtyMemory(null));
    }

    private synchronized void executeSave(boolean appendModeEnabled, int dirtyMemory) {
        DataBuffer chunkBody = DataBuffer.getOrCreate(dirtyMemory);
        boolean appendMode = false;
        try {
            Chunk c;
            Chunk lastChunk = chunkManager.getLastChunk();
            if (appendModeEnabled && lastChunk != null
                    && lastChunk.fileStorage.size() + dirtyMemory < maxChunkSize) {
                c = lastChunk;
                appendMode = true;
            } else {
                c = chunkManager.createChunk();
                c.fileStorage = getFileStorage(c.fileName);
            }
            c.mapSize = map.size();

            PageInfo pInfo = map.getRootPageRef().getPageInfo();
            long pos = pInfo.page.writeUnsavedRecursive(pInfo, c, chunkBody);
            c.rootPagePos = pos;
            c.write(chunkBody, appendMode, chunkManager);
            if (!appendMode) {
                chunkManager.addChunk(c);
                chunkManager.setLastChunk(c);
            }
        } catch (IllegalStateException e) {
            throw panic(e);
        } finally {
            chunkBody.close();
        }
    }

    public boolean isInMemory() {
        return map.isInMemory();
    }

    public FileStorage getFileStorage(int chunkId) {
        String chunkFileName = mapBaseDir + File.separator + chunkManager.getChunkFileName(chunkId);
        return openFileStorage(chunkFileName);
    }

    private FileStorage getFileStorage(String chunkFileName) {
        chunkFileName = mapBaseDir + File.separator + chunkFileName;
        return openFileStorage(chunkFileName);
    }

    private FileStorage openFileStorage(String chunkFileName) {
        return FileStorage.open(chunkFileName, map.getConfig());
    }

    InputStream getInputStream(FilePath file) {
        return chunkManager.getChunkInputStream(file);
    }
}
