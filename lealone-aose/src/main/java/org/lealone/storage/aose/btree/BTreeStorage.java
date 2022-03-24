/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.Callable;

import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.aose.btree.PageOperations.CallableOperation;
import org.lealone.storage.cache.CacheLongKeyLIRS;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

/**
 * A persistent storage for btree map.
 */
public class BTreeStorage {

    final BTreeMap<Object, Object> map;
    final String mapBaseDir;

    private final ChunkManager chunkManager;

    private final int pageSplitSize;
    private final int minFillRate;

    private final UncaughtExceptionHandler backgroundExceptionHandler;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected number of entries.
     */
    private final CacheLongKeyLIRS<BTreePage> cache;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for high).
     * Even if disabled, the storage may contain (old) compressed pages.
     */
    private final int compressionLevel;
    private Compressor compressorFast;
    private Compressor compressorHigh;

    private boolean closed;
    private volatile boolean hasUnsavedChanges;

    /**
     * Create and open the storage.
     * 
     * @param map the map to use
     * @throws IllegalStateException if the file is corrupt, or an exception occurred while opening
     */
    BTreeStorage(BTreeMap<Object, Object> map) {
        this.map = map;
        pageSplitSize = getIntValue("pageSplitSize", 16 * 1024);
        minFillRate = getIntValue("minFillRate", 30);
        compressionLevel = getIntValue("compress", 0);
        backgroundExceptionHandler = (UncaughtExceptionHandler) map.config.get("backgroundExceptionHandler");

        int mb = getIntValue("cacheSize", 16);
        if (mb > 0) {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = mb * 1024L * 1024L;
            cache = new CacheLongKeyLIRS<>(cc);
        } else {
            cache = null; // 当 cacheSize <= 0 时禁用缓存
        }

        chunkManager = new ChunkManager(this);
        mapBaseDir = map.getStorage().getStoragePath() + File.separator + map.getName();
        if (!FileUtils.exists(mapBaseDir))
            FileUtils.createDirectories(mapBaseDir);
        else {
            chunkManager.init();
        }
    }

    private int getIntValue(String key, int defaultValue) {
        Object value = map.config.get(key);
        return value != null ? (Integer) value : defaultValue;
    }

    IllegalStateException panic(int errorCode, String message, Object... arguments) {
        IllegalStateException e = DataUtils.newIllegalStateException(errorCode, message, arguments);
        return panic(e);
    }

    IllegalStateException panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        closeImmediately();
        return e;
    }

    BTreeChunk getLastChunk() {
        return chunkManager.getLastChunk();
    }

    /**
     * Get the chunk for the given position.
     * 
     * @param pos the position
     * @return the chunk
     */
    BTreeChunk getChunk(long pos) {
        return chunkManager.getChunk(pos);
    }

    /**
     * Put the page in the cache.
     * 
     * @param pos the page position
     * @param page the page
     * @param memory the memory used
     */
    void cachePage(long pos, BTreePage page, int memory) {
        if (cache != null) {
            cache.put(pos, page, memory);
        }
    }

    /**
     * Read a page.
     * 
     * @param pos the page position
     * @return the page
     */
    BTreePage readPage(long pos) {
        return readPage(null, pos);
    }

    BTreePage readPage(PageReference ref, long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        } else if (ref != null && pos < 0) {
            return ref.readRemotePage(map);
        }
        return readLocalPageAsync(pos);
    }

    private SQLStatementExecutor getSQLStatementExecutor() {
        Thread t = Thread.currentThread();
        if (t instanceof SQLStatementExecutor)
            return (SQLStatementExecutor) t;
        else
            return null;
    }

    private BTreePage readLocalPageAsync(final long pos) {
        final SQLStatementExecutor sqlStatementExecutor = getSQLStatementExecutor();
        if (sqlStatementExecutor == null)
            return readLocalPageSync(pos);
        Callable<BTreePage> task = null;
        boolean taskInQueue = false;
        while (true) {
            BTreePage p = getPageFromCache(pos);
            if (p != null)
                return p;

            if (task == null) {
                task = new Callable<BTreePage>() {
                    @Override
                    public BTreePage call() throws Exception {
                        BTreePage p = readLocalPageSync(pos);
                        if (sqlStatementExecutor != null)
                            sqlStatementExecutor.wakeUp();
                        return p;
                    }
                };
            }

            if (sqlStatementExecutor != null && (Thread.currentThread() == sqlStatementExecutor)) {
                if (!taskInQueue) {
                    map.pohFactory.addPageOperation(new CallableOperation(task));
                    taskInQueue = true;
                }
                sqlStatementExecutor.executeNextStatement();
                continue;
            } else {
                try {
                    return task.call();
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }
    }

    private BTreePage getPageFromCache(long pos) {
        return cache == null ? null : cache.get(pos);
    }

    private BTreePage readLocalPageSync(long pos) {
        BTreePage p = getPageFromCache(pos);
        if (p != null)
            return p;
        BTreeChunk c = getChunk(pos);
        long filePos = BTreeChunk.getFilePos(PageUtils.getPageOffset(pos));
        int pageLength = c.getPageLength(pos);
        p = BTreePage.read(map, c.fileStorage, pos, filePos, pageLength);
        cachePage(pos, p, p.getMemory());
        return p;
    }

    /**
     * Remove a page.
     * 
     * @param pos the position of the page
     * @param memory the memory usage
     */
    void removePage(long pos, int memory) {
        hasUnsavedChanges = true;

        // we need to keep temporary pages
        if (pos == 0) {
            return;
        }

        chunkManager.addRemovedPage(pos);

        if (cache != null) {
            if (PageUtils.isLeafPage(pos)) {
                // keep nodes in the cache, because they are still used for garbage collection
                cache.remove(pos);
            }
        }
    }

    int getCompressionLevel() {
        return compressionLevel;
    }

    Compressor getCompressorFast() {
        if (compressorFast == null) {
            compressorFast = new CompressLZF();
        }
        return compressorFast;
    }

    Compressor getCompressorHigh() {
        if (compressorHigh == null) {
            compressorHigh = new CompressDeflate();
        }
        return compressorHigh;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    public int getMinFillRate() {
        return minFillRate;
    }

    /**
     * Get the maximum cache size, in MB.
     * 
     * @return the cache size
     */
    public int getCacheSize() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getMaxMemory() / 1024 / 1024);
    }

    /**
     * Set the read cache size in MB.
     * 
     * @param mb the cache size in MB.
     */
    public void setCacheSize(int mb) {
        if (cache != null) {
            cache.setMaxMemory(mb * 1024 * 1024L);
            cache.clear();
        }
    }

    long getDiskSpaceUsed() {
        return FileUtils.folderSize(new File(mapBaseDir));
    }

    long getMemorySpaceUsed() {
        if (cache != null)
            return cache.getUsedMemory();
        else
            return 0;
    }

    /**
     * Remove this storage.
     */
    synchronized void remove() {
        closeImmediately();
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
     * This method ignores all errors.
     */
    private void closeImmediately() {
        try {
            closeStorage(true);
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStorage(boolean immediate) {
        if (closed) {
            return;
        }
        if (!immediate)
            save();
        closed = true;
        synchronized (this) {
            chunkManager.close();
            // release memory early - this is important when called
            // because of out of memory
            if (cache != null)
                cache.clear();
        }
    }

    void setUnsavedChanges(boolean b) {
        hasUnsavedChanges = b;
    }

    /**
     * Check whether there are any unsaved changes.
     * 
     * @return if there are any changes
     */
    private boolean hasUnsavedChanges() {
        boolean b = hasUnsavedChanges;
        hasUnsavedChanges = false;
        return b;
    }

    /**
     * Save all changes and persist them to disk.
     * This method does nothing if there are no unsaved changes.
     */
    synchronized void save() {
        if (closed) {
            return;
        }
        if (map.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "This storage is read-only");
        }
        if (!hasUnsavedChanges()) {
            return;
        }
        try {
            executeSave(false);
            new ChunkCompactor(this, chunkManager).executeCompact();
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    synchronized void forceSave() {
        if (closed) {
            return;
        }
        executeSave(true);
    }

    synchronized void executeSave(boolean force) {
        DataBuffer chunkBody = DataBuffer.create();
        try {
            BTreeChunk c = chunkManager.createChunk();
            c.fileStorage = createFileStorage(c.fileName);
            c.mapSize = map.size();

            BTreePage p = map.root;
            // 如果不写，rootPagePos会是0，重新打开时会报错
            // if (p.getTotalCount() > 0 || force) {
            p.writeUnsavedRecursive(c, chunkBody);
            c.rootPagePos = p.getPos();
            // p.writeEnd();
            // }

            c.write(chunkBody, chunkManager.getRemovedPages());

            chunkManager.addChunk(c);
            chunkManager.setLastChunk(c);
        } catch (IllegalStateException e) {
            throw panic(e);
        } finally {
            chunkBody.close();
        }
    }

    FileStorage getFileStorage(int chunkId) {
        String chunkFileName = mapBaseDir + File.separator + chunkManager.getChunkFileName(chunkId);
        return openFileStorage(chunkFileName);
    }

    private FileStorage createFileStorage(String chunkFileName) {
        chunkFileName = mapBaseDir + File.separator + chunkFileName;
        return openFileStorage(chunkFileName);
    }

    private FileStorage openFileStorage(String chunkFileName) {
        FileStorage fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, map.config);
        return fileStorage;
    }
}
