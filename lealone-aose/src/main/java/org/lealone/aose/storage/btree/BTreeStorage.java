/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.aose.storage.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.aose.storage.AOStorage;
import org.lealone.aose.storage.AOStorageService;
import org.lealone.aose.storage.btree.BTreePage.PageReference;
import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.BitField;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.New;
import org.lealone.db.DataBuffer;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;
import org.lealone.storage.cache.CacheLongKeyLIRS;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;

/**
 * A persistent storage for map.
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeStorage {

    /**
     * The block size (physical sector size) of the disk. The chunk header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    public static final int BLOCK_SIZE = 4 * 1024;
    public static final int CHUNK_HEADER_BLOCKS = 2;
    public static final int CHUNK_HEADER_SIZE = CHUNK_HEADER_BLOCKS * BLOCK_SIZE;

    static long getFilePos(int offset) {
        long filePos = offset + CHUNK_HEADER_SIZE;
        if (filePos < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Negative position {0}", filePos);
        }
        return filePos;
    }

    private final BTreeMap<Object, Object> map;
    private final String btreeStorageName;

    private final ConcurrentHashMap<Long, String> hashCodeToHostIdMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, BTreeChunk> chunks = new ConcurrentHashMap<>();
    private final BitField chunkIds = new BitField();
    private final RandomAccessFile chunkMetaData;

    private final TreeSet<Long> removedPages = new TreeSet<>();

    /**
    * The newest chunk. If nothing was stored yet, this field is not set.
    */
    BTreeChunk lastChunk;

    private final int pageSplitSize;
    private final int minFillRate;
    private final UncaughtExceptionHandler backgroundExceptionHandler;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
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
    private IllegalStateException panicException;
    private DataBuffer writeBuffer;

    private volatile boolean hasUnsavedChanges;

    /**
     * Create and open the storage.
     * 
     * @param map the map to use
     * @throws IllegalStateException if the file is corrupt, or an exception occurred while opening
     */
    BTreeStorage(BTreeMap<Object, Object> map) {
        this.map = map;
        Map<String, Object> config = map.config;

        Object value = config.get("pageSplitSize");
        pageSplitSize = value != null ? (Integer) value : 16 * 1024;

        value = config.get("minFillRate");
        minFillRate = value != null ? (Integer) value : 30;

        backgroundExceptionHandler = (UncaughtExceptionHandler) config.get("backgroundExceptionHandler");

        value = config.get("cacheSize");
        int mb = value == null ? 16 : (Integer) value;
        if (mb > 0) {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = mb * 1024L * 1024L;
            cache = new CacheLongKeyLIRS<>(cc);
        } else {
            cache = null;
        }

        value = config.get("compress");
        compressionLevel = value == null ? 0 : (Integer) value;

        btreeStorageName = (String) config.get("storageName") + File.separator + map.getName();
        if (!FileUtils.exists(btreeStorageName))
            FileUtils.createDirectories(btreeStorageName);
        else {
            for (int id : getAllChunkIds()) {
                chunkIds.set(id);
            }
        }

        String file = btreeStorageName + File.separator + "chunkMetaData";
        try {
            chunkMetaData = new RandomAccessFile(file, "rw");
            int lastChunkId = readLastChunkId();
            if (lastChunkId > 0) {
                lastChunk = readChunkHeader(lastChunkId);
            } else {
                lastChunk = null;
            }
        } catch (IllegalStateException e) {
            throw panic(e);
        } catch (IOException e) {
            throw panic(DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED,
                    "Failed to read chunkMetaData: {0}", file, e));
        }
    }

    private List<Integer> getAllChunkIds() {
        List<Integer> ids = New.arrayList();
        String[] files = new File(btreeStorageName).list();
        if (files != null && files.length > 0) {
            for (String f : files) {
                if (f.endsWith(AOStorage.SUFFIX_AO_FILE)) {
                    int id = Integer.parseInt(f.substring(0, f.length() - AOStorage.SUFFIX_AO_FILE_LENGTH));
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    private synchronized int readLastChunkId() throws IOException {
        if (chunkMetaData.length() <= 0)
            return 0;
        int lastChunkId = chunkMetaData.readInt();

        int removedPagesCount = chunkMetaData.readInt();
        for (int i = 0; i < removedPagesCount; i++)
            removedPages.add(chunkMetaData.readLong());

        int hashCodeToHostIdMapSize = chunkMetaData.readInt();
        for (int i = 0; i < hashCodeToHostIdMapSize; i++)
            addHostIds(chunkMetaData.readUTF());
        return lastChunkId;
    }

    private synchronized TreeSet<Long> readRemovedPages() {
        try {
            TreeSet<Long> removedPages = new TreeSet<>();
            if (chunkMetaData.length() > 8) {
                chunkMetaData.seek(4);
                int removedPagesCount = chunkMetaData.readInt();

                for (int i = 0; i < removedPagesCount; i++)
                    removedPages.add(chunkMetaData.readLong());
            }

            return removedPages;
        } catch (IOException e) {
            throw panic(DataUtils.newIllegalStateException(DataUtils.ERROR_READING_FAILED, "Failed to readRemovedPages",
                    e));
        }
    }

    private synchronized void writeChunkMetaData(int lastChunkId, TreeSet<Long> removedPages) {
        try {
            chunkMetaData.setLength(0);
            chunkMetaData.seek(0);
            chunkMetaData.writeInt(lastChunkId);
            chunkMetaData.writeInt(removedPages.size());
            for (long pos : removedPages) {
                chunkMetaData.writeLong(pos);
            }
            chunkMetaData.writeInt(hashCodeToHostIdMap.size());
            for (String hostId : hashCodeToHostIdMap.values()) {
                chunkMetaData.writeUTF(hostId);
            }
            // chunkMetaData.setLength(4 + 4 + removedPages.size() * 8);
            chunkMetaData.getFD().sync();
        } catch (IOException e) {
            throw panic(DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Failed to writeChunkMetaData", e));
        }
    }

    private IllegalStateException panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        panicException = e;
        closeImmediately();
        return e;
    }

    private FileStorage getFileStorage(int chunkId) {
        String chunkFileName = btreeStorageName + File.separator + chunkId + AOStorage.SUFFIX_AO_FILE;
        FileStorage fileStorage = new FileStorage();
        fileStorage.open(chunkFileName, map.config);
        return fileStorage;
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

    /**
     * Get the chunk for the given position.
     * 
     * @param pos the position
     * @return the chunk
     */
    BTreeChunk getChunk(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
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
        // return readLocalPageSync(pos);
        return readLocalPageAsync(pos);
    }

    private BTreePage readLocalPageAsync(final long pos) {
        Callable<BTreePage> task = null;
        boolean taskInQueue = false;
        final SQLStatementExecutor sqlStatementExecutor = SQLEngineManager.getInstance().getSQLStatementExecutor();
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
                    AOStorageService.submitTask(task);
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
        long filePos = getFilePos(DataUtils.getPageOffset(pos));
        long maxPos = c.blockCount * BLOCK_SIZE;
        p = BTreePage.read(c.fileStorage, pos, map, filePos, maxPos);
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

        synchronized (removedPages) {
            removedPages.add(pos);
        }

        if (cache != null) {
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                // keep nodes in the cache, because they are still used for
                // garbage collection
                cache.remove(pos);
            }
        }
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

    int getCompressionLevel() {
        return compressionLevel;
    }

    public int getPageSplitSize() {
        return pageSplitSize;
    }

    /**
     * Get the amount of memory used for caching, in MB.
     * 
     * @return the amount of memory used for caching
     */
    public int getCacheSizeUsed() {
        if (cache == null) {
            return 0;
        }
        return (int) (cache.getUsedMemory() / 1024 / 1024);
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
            cache.setMaxMemory((long) mb * 1024 * 1024);
            cache.clear();
        }
    }

    /**
     * Remove this storage.
     */
    synchronized void remove() {
        checkOpen();
        closeImmediately();
        FileUtils.deleteRecursive(btreeStorageName, true);
    }

    boolean isClosed() {
        return closed;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This storage is closed", panicException);
        }
    }

    /**
     * Close the file and the storage. Unsaved changes are written to disk first.
     */
    void close() {
        closeStorage();
    }

    /**
     * Close the file and the storage, without writing anything.
     * This method ignores all errors.
     */
    private void closeImmediately() {
        try {
            closeStorage();
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStorage() {
        if (closed) {
            return;
        }
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

            try {
                chunkMetaData.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Check whether there are any unsaved changes.
     * 
     * @return if there are any changes
     */
    private boolean hasUnsavedChanges() {
        checkOpen();
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
            executeCompact(executeSave(false));
        } catch (IllegalStateException e) {
            throw panic(e);
        }
    }

    synchronized void forceSave() {
        executeSave(true);
    }

    private TreeSet<Long> executeSave(boolean force) {
        DataBuffer buff = getDataBuffer();
        int id = chunkIds.nextClearBit(1);
        chunkIds.set(id);
        BTreeChunk c = new BTreeChunk(id);
        chunks.put(c.id, c);
        c.pagePositions = new ArrayList<>();
        c.pageLengths = new ArrayList<>();

        BTreePage p;
        TreeSet<Long> removedPages;
        synchronized (this.removedPages) {
            removedPages = new TreeSet<>(this.removedPages);
            this.removedPages.clear();
            p = map.root;
        }
        if (p.getTotalCount() > 0 || force) {
            p.writeUnsavedRecursive(c, buff);
            c.rootPagePos = p.getPos();
            p.writeEnd();
        }

        c.pagePositionsOffset = buff.position();
        for (long pos : c.pagePositions)
            buff.putLong(pos);
        c.pageLengthsOffset = buff.position();
        for (int pos : c.pageLengths)
            buff.putInt(pos);

        int chunkBodyLength = buff.position();
        chunkBodyLength = MathUtils.roundUpInt(chunkBodyLength, BLOCK_SIZE);
        buff.limit(chunkBodyLength);
        buff.position(0);

        c.blockCount = chunkBodyLength / BLOCK_SIZE + CHUNK_HEADER_BLOCKS; // include chunk header(2 blocks).
        c.fileStorage = getFileStorage(c.id);

        // chunk header
        writeChunkHeader(c);
        // chunk body
        write(c.fileStorage, CHUNK_HEADER_SIZE, buff.getBuffer());
        c.fileStorage.sync();

        removedPages.addAll(readRemovedPages());
        writeChunkMetaData(c.id, removedPages);

        releaseDataBuffer(buff);
        lastChunk = c;
        return removedPages;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the storage
     * before calling the method and until after using the buffer.
     * 
     * @return the buffer
     */
    private DataBuffer getDataBuffer() {
        DataBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = DataBuffer.create();
        }
        return buff;
    }

    /**
     * Release a buffer for writing. This caller must synchronize on the storage
     * before calling the method and until after using the buffer.
     * 
     * @param buff the buffer than can be re-used
     */
    private void releaseDataBuffer(DataBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    // //////////////////////////////// Compact BEGIN ///////////////////////////////////
    /**
     * Try to increase the fill rate by re-writing partially full chunks. 
     * Chunks with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the minimum fill rate, nothing is done.
     */
    private void executeCompact(TreeSet<Long> removedPages) {
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
                    removedPages = executeSave(false);
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
            removedPages.removeAll(c.pagePositions);
        }

        if (size > removedPages.size()) {
            writeChunkMetaData(lastChunk.id, removedPages);
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
            for (int i = 0, size = c.pagePositions.size(); i < size; i++) {
                if (!removedPages.contains(c.pagePositions.get(i))) {
                    c.sumOfLivePageLength += c.pageLengths.get(i);
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
        List<BTreeChunk> old = New.arrayList();
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
            for (int i = 0, size = c.pagePositions.size(); i < size; i++) {
                long pos = c.pagePositions.get(i);
                if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
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

    // //////////////////////////////// Compact END /////////////////////////////////////

    void readPagePositions(BTreeChunk c) {
        int size = c.pageCount;
        if (c.pagePositions == null) {
            ByteBuffer buffer = c.fileStorage.readFully(getFilePos(c.pagePositionsOffset), size * 8);
            c.pagePositions = new ArrayList<Long>(size);
            for (int i = 0; i < size; i++) {
                c.pagePositions.add(buffer.getLong());
            }
        }
        if (c.pageLengths == null) {
            ByteBuffer buffer = c.fileStorage.readFully(getFilePos(c.pageLengthsOffset), size * 4);
            c.pageLengths = new ArrayList<Integer>(size);
            for (int i = 0; i < size; i++) {
                c.pageLengths.add(buffer.getInt());
            }
        }
    }

    void addHostIds(Collection<String> hostIds) {
        if (hostIds != null)
            addHostIds(hostIds.toArray(new String[0]));
    }

    void addHostIds(String... hostIds) {
        if (hostIds != null) {
            for (String hostId : hostIds) {
                if (hostId != null) {
                    hashCodeToHostIdMap.put(Long.valueOf(getHostIdHashCode(hostId)), hostId);
                }
            }
        }
    }

    String findHostId(long hashCode) {
        return hashCodeToHostIdMap.get(hashCode);
    }

    long getHostIdHashCode(String hostId) {
        int hashCode = hostId.hashCode();
        // 统一取负值，这样可以区分是不是真实的pos
        if (hashCode > 0) {
            hashCode = -hashCode;
        }
        return hashCode;
    }

    long getDiskSpaceUsed() {
        return org.lealone.aose.util.FileUtils.folderSize(new File(btreeStorageName));
    }

    long getMemorySpaceUsed() {
        if (cache != null)
            return cache.getUsedMemory();
        else
            return 0;
    }
}
