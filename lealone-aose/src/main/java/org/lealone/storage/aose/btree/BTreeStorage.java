/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.io.File;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.StorageSetting;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.aose.btree.chunk.ChunkCompactor;
import org.lealone.storage.aose.btree.chunk.ChunkManager;
import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.Page.SavedPage;
import org.lealone.storage.aose.btree.page.PageInfo;
import org.lealone.storage.aose.btree.page.PageReference;
import org.lealone.storage.aose.btree.page.PageUtils;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

/**
 * A persistent storage for btree map.
 */
public class BTreeStorage {

    private final BTreeMap<?, ?> map;
    private final String mapBaseDir;

    private final ChunkManager chunkManager;

    private final int pageSplitSize;
    private final int minFillRate;

    private final BTreeGC bgc;

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
    BTreeStorage(BTreeMap<?, ?> map) {
        this.map = map;
        pageSplitSize = getIntValue(StorageSetting.PAGE_SPLIT_SIZE.name(), 16 * 1024);
        int minFillRate = getIntValue(StorageSetting.MIN_FILL_RATE.name(), 30);
        if (minFillRate > 50) // 超过50没有实际意义
            minFillRate = 50;
        this.minFillRate = minFillRate;
        compressionLevel = parseCompressionLevel();

        // 原先是16M (16 * 1024 * 1024)，改用动态回收page后默认就不需要了
        int cacheSize = getIntValue(StorageSetting.CACHE_SIZE.name(), -1);
        if (cacheSize > 0 && cacheSize < pageSplitSize)
            cacheSize = pageSplitSize * 2;
        bgc = new BTreeGC(map, cacheSize);

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
        Object value = map.getConfig(StorageSetting.COMPRESS.name());
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

    Chunk getLastChunk() {
        return chunkManager.getLastChunk();
    }

    /**
     * Get the chunk for the given position.
     * 
     * @param pos the position
     * @return the chunk
     */
    public Chunk getChunk(long pos) {
        return chunkManager.getChunk(pos);
    }

    // ChunkCompactor在重写chunk中的page时会用到
    public void markDirtyLeafPage(long pos) {
        Page leaf = readPage(null, new PageReference(pos), pos, false);
        Object key = leaf.getKey(0);
        Page p = map.getRootPage();
        while (p.isNode()) {
            p.markDirty(false);
            int index = p.getPageIndex(key);
            PageReference ref = p.getChildPageReference(index);
            if (ref.isNodePage()) {
                p = p.getChildPage(index);
            } else {
                if (ref.getPage() == null) {
                    ref.replacePage(leaf);
                    leaf.setRef(ref);
                } else {
                    leaf = ref.getPage();
                }
                break;
            }
        }
        leaf.markDirty(true);
    }

    public Page readPage(PageInfo pInfoOld, PageReference ref) {
        return readPage(pInfoOld, ref, ref.getPos(), true);
    }

    public Page readPage(PageInfo pInfoOld, PageReference ref, long pos) {
        return readPage(pInfoOld, ref, pos, true);
    }

    private Page readPage(PageInfo pInfoOld, PageReference ref, long pos, boolean gc) {
        if (ref == null) {
            DbException.throwInternalError();
        }
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        Chunk c = getChunk(pos);
        long filePos = Chunk.getFilePos(PageUtils.getPageOffset(pos));
        int pageLength = c.getPageLength(pos);
        if (pageLength < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1} ", pageLength, filePos);
        }
        ByteBuffer buff = c.fileStorage.readFully(filePos, pageLength);
        Page p = readPage(pInfoOld, ref, pos, buff, pageLength);
        if (gc)
            gcIfNeeded(p.getTotalMemory());
        return p;
    }

    public Page readPage(PageInfo pInfoOld, PageReference ref, long pos, ByteBuffer buff,
            int pageLength) {
        int type = PageUtils.getPageType(pos);
        Page p = Page.create(map, type);
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);

        PageInfo pInfo = new PageInfo();
        pInfo.page = p;
        pInfo.pos = pos;
        pInfo.buff = buff;
        pInfo.pageLength = pageLength;

        p.read(pInfo, buff, chunkId, offset, pageLength, false);
        if (type != PageUtils.PAGE_TYPE_COLUMN) // ColumnPage还没有读完
            buff.flip();

        if (ref.replacePage(pInfoOld, pInfo)) {
            p.setRef(ref);
        }
        ref.updateTime();
        return ref.getPage();
    }

    public void gcIfNeeded(long delta) {
        bgc.gcIfNeeded(delta);
    }

    public void gc() {
        bgc.gc();
    }

    public void addDirtyMemory(int mem) {
        bgc.addDirtyMemory(mem);
    }

    /**
     * Remove a page.
     * 
     * @param pos the position of the page
     * @param memory the memory usage
     */
    public void removePage(long pos, int memory) {
        hasUnsavedChanges = true;
        // we need to keep temporary pages
        if (pos == 0) {
            return;
        }
        chunkManager.addRemovedPage(pos);
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

    public long getDirtyMemorySpaceUsed() {
        return bgc.getDirtyMemory();
    }

    synchronized void clear() {
        if (map.isInMemory())
            return;
        bgc.close();
        chunkManager.close();
        hasUnsavedChanges = true;
        // FileUtils.deleteRecursive(mapBaseDir, true);
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

    public void setUnsavedChanges(boolean b) {
        hasUnsavedChanges = b;
    }

    public boolean hasUnsavedChanges() {
        return hasUnsavedChanges;
    }

    /**
     * Save all changes and persist them to disk.
     * This method does nothing if there are no unsaved changes.
     */
    synchronized void save() {
        if (!hasUnsavedChanges || closed || map.isInMemory()) {
            return;
        }
        if (map.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "This storage is read-only");
        }
        bgc.resetDirtyMemory();
        hasUnsavedChanges = false;
        try {
            executeSave();
            new ChunkCompactor(this, chunkManager).executeCompact();
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    public synchronized void executeSave() {
        DataBuffer chunkBody = DataBuffer.create();
        try {
            Chunk c = chunkManager.createChunk();
            c.fileStorage = getFileStorage(c.fileName);
            c.mapSize = map.size();

            LinkedList<SavedPage> savedPages = new LinkedList<>();

            Page p = map.getRootPage();
            p.writeUnsavedRecursive(c, chunkBody, savedPages);
            c.rootPagePos = p.getPos();

            c.write(chunkBody, chunkManager.getRemovedPages());

            chunkManager.addChunk(c);
            chunkManager.setLastChunk(c);

            // 保存数据后再释放资源
            for (SavedPage savedPage : savedPages) {
                savedPage.release();
            }
        } catch (IllegalStateException e) {
            throw panic(e);
        } finally {
            chunkBody.close();
        }
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
