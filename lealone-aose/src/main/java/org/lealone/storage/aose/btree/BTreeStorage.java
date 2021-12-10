/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.aose.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.BitField;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.db.DataBuffer;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.PageOperations.CallableOperation;
import org.lealone.storage.cache.CacheLongKeyLIRS;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

/**
 * A persistent storage for btree map.
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeStorage {

    /**
     * The block size (physical sector size) of the disk. The chunk header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    static final int BLOCK_SIZE = 4 * 1024;
    private static final int CHUNK_HEADER_BLOCKS = 2;
    static final int CHUNK_HEADER_SIZE = CHUNK_HEADER_BLOCKS * BLOCK_SIZE;

    private static long getFilePos(int offset) {
        long filePos = offset + CHUNK_HEADER_SIZE;
        if (filePos < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Negative position {0}", filePos);
        }
        return filePos;
    }

    private final BTreeMap<Object, Object> map;
    private final String btreeStoragePath;

    private final ConcurrentHashMap<Integer, BTreeChunk> chunks = new ConcurrentHashMap<>();
    private final BitField chunkIds = new BitField();
    private final ChunkMetaData chunkMetaData;

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

        btreeStoragePath = map.getStorage().getStoragePath() + File.separator + map.getName();
        if (!FileUtils.exists(btreeStoragePath))
            FileUtils.createDirectories(btreeStoragePath);
        else {
            for (int id : getAllChunkIds()) {
                chunkIds.set(id);
            }
        }
        chunkMetaData = new ChunkMetaData();
    }

    private int getIntValue(String key, int defaultValue) {
        Object value = map.config.get(key);
        return value != null ? (Integer) value : defaultValue;
    }

    private List<Integer> getAllChunkIds() {
        String[] files = new File(btreeStoragePath).list();
        List<Integer> ids = new ArrayList<>(files.length);
        for (String f : files) {
            if (f.endsWith(AOStorage.SUFFIX_AO_FILE)) {
                int id = Integer.parseInt(f.substring(0, f.length() - AOStorage.SUFFIX_AO_FILE_LENGTH));
                ids.add(id);
            }
        }
        return ids;
    }

    private FileStorage getFileStorage(int chunkId) {
        String chunkFileName = btreeStoragePath + File.separator + chunkId + AOStorage.SUFFIX_AO_FILE;
        FileStorage fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, map.config);
        return fileStorage;
    }

    private static void readPagePositions(BTreeChunk c) {
        if (!c.pagePositionToLengthMap.isEmpty())
            return;
        int size = c.pageCount;
        ByteBuffer buff = c.fileStorage.readFully(getFilePos(c.pagePositionAndLengthOffset), size * 8 + size * 4);
        for (int i = 0; i < size; i++) {
            long position = buff.getLong();
            int length = buff.getInt();
            c.pagePositionToLengthMap.put(position, length);
        }
    }

    private synchronized BTreeChunk readChunkHeader(int chunkId) {
        FileStorage fileStorage = getFileStorage(chunkId);
        BTreeChunk chunk = null;
        ByteBuffer chunkHeaderBlocks = fileStorage.readFully(0, CHUNK_HEADER_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            chunkHeaderBlocks.get(buff);
            try {
                String s = new String(buff, 0, BLOCK_SIZE, DataUtils.LATIN).trim();
                HashMap<String, String> m = DataUtils.parseMap(s);
                int blockSize = DataUtils.readHexInt(m, "blockSize", BLOCK_SIZE);
                if (blockSize != BLOCK_SIZE) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                            "Block size {0} is currently not supported", blockSize);
                }
                int check = DataUtils.readHexInt(m, "fletcher", 0);
                m.remove("fletcher");
                s = s.substring(0, s.lastIndexOf("fletcher") - 1);
                byte[] bytes = s.getBytes(DataUtils.LATIN);
                int checksum = DataUtils.getFletcher32(bytes, bytes.length);
                if (check != checksum) {
                    continue;
                }
                chunk = BTreeChunk.fromString(s);
                break;
            } catch (Exception e) {
                continue;
            }
        }
        if (chunk == null) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Storage header is corrupt: {0}",
                    fileStorage);
        }

        chunk.fileStorage = fileStorage;
        chunks.put(chunk.id, chunk);
        readPagePositions(chunk);
        return chunk;
    }

    private synchronized void writeChunkHeader(BTreeChunk chunk) {
        StringBuilder buff = chunk.asStringBuilder();
        byte[] bytes = buff.toString().getBytes(DataUtils.LATIN);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append("\n");
        bytes = buff.toString().getBytes(DataUtils.LATIN);
        ByteBuffer header = ByteBuffer.allocate(CHUNK_HEADER_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(chunk.fileStorage, 0, header);
    }

    private synchronized void write(FileStorage fileStorage, long pos, ByteBuffer buffer) {
        try {
            fileStorage.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    private IllegalStateException panic(int errorCode, String message, Object... arguments) {
        IllegalStateException e = DataUtils.newIllegalStateException(errorCode, message, arguments);
        return panic(e);
    }

    private IllegalStateException panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        closeImmediately();
        return e;
    }

    BTreeChunk getLastChunk() {
        return chunkMetaData.getLastChunk();
    }

    /**
     * Get the chunk for the given position.
     * 
     * @param pos the position
     * @return the chunk
     */
    BTreeChunk getChunk(long pos) {
        int chunkId = PageUtils.getPageChunkId(pos);
        BTreeChunk c = chunks.get(chunkId);
        if (c == null)
            c = readChunkHeader(chunkId);
        if (c == null)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} not found", chunkId);
        return c;
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
        long filePos = getFilePos(PageUtils.getPageOffset(pos));
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

        chunkMetaData.addRemovedPage(pos);

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
        return FileUtils.folderSize(new File(btreeStoragePath));
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
        FileUtils.deleteRecursive(btreeStoragePath, true);
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
            for (BTreeChunk c : chunks.values()) {
                if (c.fileStorage != null)
                    c.fileStorage.close();
            }
            chunks.clear();

            // release memory early - this is important when called
            // because of out of memory
            if (cache != null)
                cache.clear();

            chunkMetaData.close();
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
            new Compactor().executeCompact();
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

    private void executeSave(boolean force) {
        int id = chunkIds.nextClearBit(1);
        chunkIds.set(id);
        BTreeChunk c = new BTreeChunk(id);
        chunks.put(id, c);

        BTreePage p = map.root;
        DataBuffer buff = DataBuffer.create();
        // 如果不写，rootPagePos会是0，重新打开时会报错
        // if (p.getTotalCount() > 0 || force) {
        p.writeUnsavedRecursive(c, buff);
        c.rootPagePos = p.getPos();
        // p.writeEnd();
        // }

        c.pagePositionAndLengthOffset = buff.position();
        for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
            buff.putLong(e.getKey()).putInt(e.getValue());
        }

        int chunkBodyLength = buff.position();
        chunkBodyLength = MathUtils.roundUpInt(chunkBodyLength, BLOCK_SIZE);
        buff.limit(chunkBodyLength);
        buff.position(0);

        c.blockCount = chunkBodyLength / BLOCK_SIZE + CHUNK_HEADER_BLOCKS; // include chunk header(2 blocks).
        c.fileStorage = getFileStorage(c.id);
        c.mapSize = map.size();

        // chunk header
        writeChunkHeader(c);
        // chunk body
        write(c.fileStorage, CHUNK_HEADER_SIZE, buff.getBuffer());
        c.fileStorage.sync();
        buff.close();

        chunkMetaData.update(c);
    }

    // chunkMetaData文件保存上一个chunk的id以及所有已经删除的page的pos
    private class ChunkMetaData {
        /**
         * The newest chunk. If nothing was stored yet, this field is not set.
         */
        private BTreeChunk lastChunk;
        private final TreeSet<Long> removedPages = new TreeSet<>();
        private final RandomAccessFile chunkMetaDataFile;

        private ChunkMetaData() {
            String file = btreeStoragePath + File.separator + "chunkMetaData";
            try {
                chunkMetaDataFile = new RandomAccessFile(file, "rw");
                if (chunkMetaDataFile.length() <= 0)
                    return;
                int lastChunkId = chunkMetaDataFile.readInt();
                int removedPagesCount = chunkMetaDataFile.readInt();
                for (int i = 0; i < removedPagesCount; i++)
                    removedPages.add(chunkMetaDataFile.readLong());
                readLastChunk(lastChunkId);
            } catch (IOException e) {
                throw panic(DataUtils.ERROR_READING_FAILED, "Failed to read chunkMetaData: {0}", file, e);
            }
        }

        private void readLastChunk(int lastChunkId) {
            try {
                if (lastChunkId > 0) {
                    lastChunk = readChunkHeader(lastChunkId);
                } else {
                    lastChunk = null;
                }
            } catch (IllegalStateException e) {
                throw panic(e);
            } catch (Exception e) {
                throw panic(DataUtils.ERROR_READING_FAILED, "Failed to read last chunk: {0}", lastChunkId, e);
            }
        }

        private BTreeChunk getLastChunk() {
            return lastChunk;
        }

        private synchronized TreeSet<Long> getRemovedPages() {
            return removedPages;
        }

        private synchronized void addRemovedPage(long pagePos) {
            removedPages.add(pagePos);
        }

        private synchronized void update(BTreeChunk lastChunk) {
            this.lastChunk = lastChunk;
            write();
        }

        private synchronized void update(TreeSet<Long> removedPages) {
            this.removedPages.clear();
            this.removedPages.addAll(removedPages);
            write();
        }

        private synchronized void write() {
            try {
                chunkMetaDataFile.setLength(0);
                chunkMetaDataFile.seek(0);
                chunkMetaDataFile.writeInt(lastChunk == null ? 0 : lastChunk.id);
                chunkMetaDataFile.writeInt(removedPages.size());
                for (long pos : removedPages) {
                    chunkMetaDataFile.writeLong(pos);
                }
                chunkMetaDataFile.getFD().sync();
            } catch (IOException e) {
                throw panic(DataUtils.ERROR_WRITING_FAILED, "Failed to write chunkMetaData", e);
            }
        }

        private synchronized void close() {
            try {
                chunkMetaDataFile.close();
            } catch (IOException e) {
            }
            removedPages.clear();
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. 
     * Chunks with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the minimum fill rate, nothing is done.
     */
    private class Compactor {

        private void executeCompact() {
            TreeSet<Long> removedPages = chunkMetaData.getRemovedPages();
            if (removedPages.isEmpty())
                return;

            removeUnusedChunks(removedPages);

            if (minFillRate <= 0)
                return;

            if (!removedPages.isEmpty()) {
                List<BTreeChunk> old = getOldChunks();
                if (!old.isEmpty()) {
                    boolean saveIfNeeded = rewrite(old, removedPages);
                    if (saveIfNeeded) {
                        executeSave(false);
                        removedPages = chunkMetaData.getRemovedPages();
                        removeUnusedChunks(removedPages);
                    }
                }
            }
        }

        private void removeUnusedChunks(TreeSet<Long> removedPages) {
            int size = removedPages.size();
            for (BTreeChunk c : findUnusedChunks(removedPages)) {
                c.fileStorage.close();
                c.fileStorage.delete();
                chunks.remove(c.id);
                chunkIds.clear(c.id);
                removedPages.removeAll(c.pagePositionToLengthMap.keySet());
            }

            if (size > removedPages.size()) {
                chunkMetaData.update(removedPages);
            }
        }

        private ArrayList<BTreeChunk> findUnusedChunks(TreeSet<Long> removedPages) {
            ArrayList<BTreeChunk> unusedChunks = new ArrayList<>();
            if (removedPages.isEmpty())
                return unusedChunks;

            readAllChunks();

            for (BTreeChunk c : chunks.values()) {
                c.sumOfLivePageLength = 0;
                boolean unused = true;
                for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                    if (!removedPages.contains(e.getKey())) {
                        c.sumOfLivePageLength += e.getValue();
                        unused = false;
                    }
                }
                if (unused)
                    unusedChunks.add(c);
            }
            return unusedChunks;
        }

        private void readAllChunks() {
            for (int id : getAllChunkIds()) {
                if (!chunks.containsKey(id)) {
                    readChunkHeader(id);
                }
            }
            for (BTreeChunk c : chunks.values()) {
                readPagePositions(c);
            }
        }

        private List<BTreeChunk> getOldChunks() {
            long maxBytesToWrite = BTreeChunk.MAX_SIZE;
            List<BTreeChunk> old = new ArrayList<>();
            for (BTreeChunk c : chunks.values()) {
                if (c.getFillRate() > minFillRate)
                    continue;
                old.add(c);
            }
            if (old.isEmpty())
                return old;

            Collections.sort(old, new Comparator<BTreeChunk>() {
                @Override
                public int compare(BTreeChunk o1, BTreeChunk o2) {
                    long comp = o1.getFillRate() - o2.getFillRate();
                    if (comp == 0) {
                        comp = o1.sumOfLivePageLength - o2.sumOfLivePageLength;
                    }
                    return Long.signum(comp);
                }
            });

            long bytes = 0;
            int index = 0;
            int size = old.size();
            for (; index < size; index++) {
                bytes += old.get(index).sumOfLivePageLength;
                if (bytes > maxBytesToWrite)
                    break;
            }
            return index == size ? old : old.subList(0, index + 1);
        }

        private boolean rewrite(List<BTreeChunk> old, TreeSet<Long> removedPages) {
            boolean saveIfNeeded = false;
            for (BTreeChunk c : old) {
                for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                    long pos = e.getKey();
                    if (PageUtils.isLeafPage(pos)) {
                        if (!removedPages.contains(pos)) {
                            BTreePage p = readPage(pos);
                            if (p.getKeyCount() > 0) {
                                Object key = p.getKey(0);
                                Object value = map.get(key);
                                if (value != null && map.replace(key, value, value))
                                    saveIfNeeded = true;
                            }
                        }
                    }
                }
            }
            return saveIfNeeded;
        }
    }
}
