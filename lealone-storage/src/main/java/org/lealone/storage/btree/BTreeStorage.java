/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.btree;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.New;
import org.lealone.storage.AOStorage;
import org.lealone.storage.cache.CacheLongKeyLIRS;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.WriteBuffer;

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

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, BTreeChunk> chunks = new ConcurrentHashMap<>();

    private final BTreeMap<Object, Object> map;
    private final String btreeStorageName;

    /**
     * How long to retain old, persisted chunks, in milliseconds. 
     * For larger or equal to zero, a chunk is never directly overwritten if unused, 
     * but instead, the unused field is set. 
     * If smaller zero, chunks are directly overwritten if unused.
     */
    private final long retentionTime;
    private final boolean reuseSpace;
    private final int pageSplitSize;
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

    private final Object compactSync = new Object();

    /**
     * The time the storage was created, in milliseconds since 1970.
     */
    private final long creationTime;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    BTreeChunk lastChunk;
    private int lastChunkId;

    private long lastTimeAbsolute;

    /**
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private long lastStoredVersion = -1;

    private long currentVersion;

    private boolean closed;
    private IllegalStateException panicException;
    private WriteBuffer writeBuffer;

    /**
     * Create and open the storage.
     * 
     * @param map the map to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    BTreeStorage(BTreeMap<Object, Object> map) {
        this.map = map;
        Map<String, Object> config = map.config;
        String storageName = (String) config.get("storageName");
        btreeStorageName = storageName + File.separator + map.getName();

        Object value = config.get("retentionTime");
        retentionTime = value == null ? 45000 : (Long) value;

        reuseSpace = config.containsKey("reuseSpace");

        value = config.get("pageSplitSize");
        pageSplitSize = value != null ? (Integer) value : 16 * 1024;

        backgroundExceptionHandler = (UncaughtExceptionHandler) config.get("backgroundExceptionHandler");

        value = config.get("cacheSize");
        int mb = value == null ? 16 : (Integer) value;
        if (mb > 0) {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = mb * 1024L * 1024L;
            cache = new CacheLongKeyLIRS<BTreePage>(cc);
        } else {
            cache = null;
        }

        value = config.get("compress");
        compressionLevel = value == null ? 0 : (Integer) value;

        lastChunkId = 0;
        if (!FileUtils.exists(btreeStorageName))
            FileUtils.createDirectories(btreeStorageName);
        String[] files = new File(btreeStorageName).list();
        if (files != null && files.length > 0) {
            for (String f : files) {
                int id = Integer.parseInt(f.substring(0, f.length() - AOStorage.SUFFIX_AO_FILE_LENGTH));
                if (id > lastChunkId)
                    lastChunkId = id;
            }
        }

        try {
            if (lastChunkId > 0)
                readLastChunk();
        } catch (IllegalStateException e) {
            panic(e);
        }

        if (lastChunk != null)
            creationTime = lastChunk.creationTime;
        else
            creationTime = getTimeAbsolute();
    }

    private long getTimeAbsolute() {
        long now = System.currentTimeMillis();
        if (lastTimeAbsolute != 0 && now < lastTimeAbsolute) {
            // time seems to have run backwards - this can happen
            // when the system time is adjusted, for example
            // on a leap second
            now = lastTimeAbsolute;
        } else {
            lastTimeAbsolute = now;
        }
        return now;
    }

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
    }

    private void panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        panicException = e;
        closeImmediately();
        throw e;
    }

    private void readLastChunk() {
        BTreeChunk last = readChunkHeader(lastChunkId);
        setLastChunk(last);
    }

    private synchronized void setLastChunk(BTreeChunk last) {
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            currentVersion = 0;
        } else {
            currentVersion = last.version;
        }
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

    private void writeChunkHeader(BTreeChunk chunk) {
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

    private void write(FileStorage fileStorage, long pos, ByteBuffer buffer) {
        try {
            fileStorage.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            panic(e);
            throw e;
        }
    }

    private void readAllChunks() {
        readAllChunks(true);
    }

    private void readAllChunks(boolean readPagePositions) {
        String[] files = new File(btreeStorageName).list();
        if (files != null && files.length > 0) {
            for (String f : files) {
                int chunkId = Integer.parseInt(f.substring(0, f.length() - AOStorage.SUFFIX_AO_FILE_LENGTH));
                if (!chunks.containsKey(chunkId)) {
                    readChunkHeader(chunkId);
                }
            }

            if (readPagePositions) {
                for (BTreeChunk c : chunks.values()) {
                    if (c.pagePositions == null) {
                        ByteBuffer pagePositions = c.fileStorage.readFully(c.pagePositionsOffset + CHUNK_HEADER_SIZE,
                                c.pageCount * 2 * 8);

                        int size = c.pageCount * 2;
                        c.pagePositions = new ArrayList<Long>();
                        for (int i = 0; i < size; i++) {
                            c.pagePositions.add(pagePositions.getLong());
                        }
                    }
                }
            }
        }
    }

    private static class UnusedPage {
        long pos;
        UnusedPage next;

        UnusedPage(long pos) {
            this.pos = pos;
        }

        int versionCount() {
            int count = 1;
            UnusedPage p = next;
            while (p != null) {
                count++;
                p = p.next;
            }
            return count;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            buff.append('(').append(pos);
            UnusedPage p = next;
            while (p != null) {
                buff.append(" -> ").append(p.pos);
                p = p.next;
            }
            return buff.append(')').toString();
        }
    }

    public ArrayList<BTreeChunk> findUnusedChunks() {
        ArrayList<BTreeChunk> unusedChunks = new ArrayList<>();
        readAllChunks();

        ArrayList<BTreeChunk> chunks = new ArrayList<>(this.chunks.values());
        // sort by id desc
        Collections.sort(chunks, new Comparator<BTreeChunk>() {
            @Override
            public int compare(BTreeChunk o1, BTreeChunk o2) {
                return Long.signum(o2.id - o1.id);
            }
        });

        HashMap<Long, UnusedPage> unusedPages = new HashMap<>();

        for (BTreeChunk c : chunks) {
            int size = c.pagePositions.size();
            for (int i = 0; i < size; i += 2) {
                long livePagePos = c.pagePositions.get(i);
                UnusedPage unusedPage = unusedPages.get(livePagePos);

                long unusedPagePos = c.pagePositions.get(i + 1);
                if (unusedPagePos > 0) {
                    if (unusedPage != null) {
                        unusedPage.next = new UnusedPage(unusedPagePos);
                    } else {
                        unusedPage = new UnusedPage(unusedPagePos);
                    }

                    unusedPages.put(unusedPagePos, unusedPage);
                }
            }
        }

        for (BTreeChunk c : chunks) {
            c.unusedPages = new HashSet<>();
            int size = c.pagePositions.size();
            int unusedPageCount = 0;
            for (int i = 0; i < size; i += 2) {
                long livePagePos = c.pagePositions.get(i);
                UnusedPage unusedPage = unusedPages.get(livePagePos);
                if (unusedPage != null && unusedPage.versionCount() > 0) { // versionsToKeep) {
                    unusedPageCount++;
                    c.unusedPages.add(livePagePos);
                } else {
                    // break;
                }
            }

            if (unusedPageCount == size / 2) {
                // long time = getTimeSinceCreation();
                // if (canOverwriteChunk(c, time))
                unusedChunks.add(c);
            }
        }
        return unusedChunks;
    }

    // private boolean canOverwriteChunk(BTreeChunk c, long time) {
    // if (retentionTime >= 0) {
    // if (c.time + retentionTime > time) {
    // return false;
    // }
    // if (c.unused == 0 || c.unused + retentionTime / 2 > time) {
    // return false;
    // }
    // }
    // return true;
    // }

    public synchronized void freeUnusedChunks() {
        ArrayList<BTreeChunk> unusedChunks = findUnusedChunks();

        for (BTreeChunk c : unusedChunks) {
            c.fileStorage.close();
            c.fileStorage.delete();
            chunks.remove(c.id);
        }
    }

    /**
     * Remove this storage.
     */
    synchronized void remove() {
        checkOpen();
        closeImmediately();
        FilePath.get(btreeStorageName).delete();
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
        if (closed) {
            return;
        }
        if (hasUnsavedChanges()) {
            save();
        }
        closeStorage();
    }

    /**
     * Close the file and the storage, without writing anything.
     * This will stop the background thread. 
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

        closed = true;
        synchronized (this) {
            for (BTreeChunk c : chunks.values()) {
                if (c.fileStorage != null)
                    c.fileStorage.close();
            }
            // release memory early - this is important when called
            // because of out of memory
            if (cache != null)
                cache.clear();
            chunks.clear();
        }
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes, otherwise it increments the current version
     * and stores the data (for file based storages).
     * <p>
     * At most one storage operation may run at any time.
     * 
     * @return the new version (incremented if there were changes)
     */
    synchronized long save() {
        if (closed) {
            return currentVersion;
        }
        if (map.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "This storage is read-only");
        }

        if (!hasUnsavedChanges()) {
            return currentVersion;
        }

        try {
            return save0();
        } catch (IllegalStateException e) {
            panic(e);
            return -1;
        }
    }

    /**
     * Check whether there are any unsaved changes.
     * 
     * @return if there are any changes
     */
    private boolean hasUnsavedChanges() {
        checkOpen();
        long v = map.getVersion();
        if (v >= 0 && v > lastStoredVersion) {
            return true;
        }
        return false;
    }

    private long save0() {
        long version = ++currentVersion;
        long time = getTimeSinceCreation();

        WriteBuffer buff = getWriteBuffer();
        BTreeChunk c = new BTreeChunk(++lastChunkId);
        chunks.put(c.id, c);
        c.time = time;
        c.version = version;
        c.pagePositions = new ArrayList<Long>();
        c.leafPagePositions = new ArrayList<Long>();

        BTreePage p = map.root;
        if (p.getTotalCount() > 0) {
            p.writeUnsavedRecursive(c, buff);
            c.rootPagePos = p.getPos();
            p.writeEnd();
        }

        c.pagePositionsOffset = buff.position();
        for (long pos : c.pagePositions)
            buff.putLong(pos);
        c.leafPagePositionsOffset = buff.position();
        for (long pos : c.leafPagePositions)
            buff.putLong(pos);

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

        for (BTreeChunk chunk : chunks.values()) {
            if (chunk.changed) {
                writeChunkHeader(chunk);
                chunk.fileStorage.sync();
                chunk.changed = false;
            }
        }

        releaseWriteBuffer(buff);
        lastStoredVersion = version - 1;
        return version;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the storage
     * before calling the method and until after using the buffer.
     * 
     * @return the buffer
     */
    private WriteBuffer getWriteBuffer() {
        WriteBuffer buff;
        if (writeBuffer != null) {
            buff = writeBuffer;
            buff.clear();
        } else {
            buff = new WriteBuffer();
        }
        return buff;
    }

    /**
     * Release a buffer for writing. This caller must synchronize on the storage
     * before calling the method and until after using the buffer.
     * 
     * @param buff the buffer than can be re-used
     */
    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. 
     * Chunks with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is done.
     * <p>
     * Please note this method will not necessarily reduce the file size, as
     * empty chunks are not overwritten.
     * 
     * @param targetFillRate the minimum percentage of live entries
     * @param write the minimum number of bytes to write
     * @return if a chunk was re-written
     */
    public boolean compact(int targetFillRate, int write) {
        if (!reuseSpace) {
            return false;
        }
        synchronized (compactSync) {
            checkOpen();
            ArrayList<BTreeChunk> old;
            synchronized (this) {
                old = compactGetOldChunks(targetFillRate, write);
            }
            if (old == null || old.isEmpty()) {
                return false;
            }
            compactRewrite(old);
            return true;
        }
    }

    private ArrayList<BTreeChunk> compactGetOldChunks(int targetFillRate, int write) {
        if (lastChunk == null) {
            // nothing to do
            return null;
        }

        readAllChunks();

        // calculate the fill rate
        long maxLengthSum = 0;
        long maxLengthLiveSum = 0;

        long time = getTimeSinceCreation();

        for (BTreeChunk c : chunks.values()) {
            // ignore young chunks, because we don't optimize those
            if (c.time + retentionTime > time) {
                continue;
            }
            maxLengthSum += c.maxLen;
            maxLengthLiveSum += c.maxLenLive;
        }
        if (maxLengthLiveSum < 0) {
            // no old data
            return null;
        }
        // the fill rate of all chunks combined
        if (maxLengthSum <= 0) {
            // avoid division by 0
            maxLengthSum = 1;
        }
        int fillRate = (int) (100 * maxLengthLiveSum / maxLengthSum);
        if (fillRate >= targetFillRate) {
            return null;
        }

        // the 'old' list contains the chunks we want to free up
        ArrayList<BTreeChunk> old = New.arrayList();
        BTreeChunk last = chunks.get(lastChunk.id);
        for (BTreeChunk c : chunks.values()) {
            // only look at chunk older than the retention time
            // (it's possible to compact chunks earlier, but right
            // now we don't do that)
            if (c.time + retentionTime > time) {
                continue;
            }
            long age = last.version - c.version + 1;
            c.collectPriority = (int) (c.getFillRate() * 1000 / age);
            old.add(c);
        }
        if (old.size() == 0) {
            return null;
        }

        // sort the list, so the first entry should be collected first
        Collections.sort(old, new Comparator<BTreeChunk>() {

            @Override
            public int compare(BTreeChunk o1, BTreeChunk o2) {
                int comp = new Integer(o1.collectPriority).compareTo(o2.collectPriority);
                if (comp == 0) {
                    comp = new Long(o1.maxLenLive).compareTo(o2.maxLenLive);
                }
                return comp;
            }
        });
        // find out up to were in the old list we need to move
        long written = 0;
        int chunkCount = 0;
        BTreeChunk move = null;
        for (BTreeChunk c : old) {
            if (move != null) {
                if (c.collectPriority > 0 && written > write) {
                    break;
                }
            }
            written += c.maxLenLive;
            chunkCount++;
            move = c;
        }
        if (chunkCount < 1) {
            return null;
        }
        // remove the chunks we want to keep from this list
        boolean remove = false;
        for (Iterator<BTreeChunk> it = old.iterator(); it.hasNext();) {
            BTreeChunk c = it.next();
            if (move == c) {
                remove = true;
            } else if (remove) {
                it.remove();
            }
        }
        return old;
    }

    private void compactRewrite(ArrayList<BTreeChunk> old) {
        for (BTreeChunk c : old) {
            for (int i = 0, size = c.pagePositions.size(); i < size; i += 2) {
                long pos = c.pagePositions.get(i);
                if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF && !c.unusedPages.contains(pos)) {
                    BTreePage p = readPage(pos);
                    if (p.getKeyCount() > 0) {
                        Object key = p.getKey(0);
                        Object value = map.get(key);
                        if (value != null) {
                            map.replace(key, value, value);
                        }
                    }
                }
            }
        }
        freeUnusedChunks();
        save();
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
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        BTreePage p = cache == null ? null : cache.get(pos);
        if (p == null) {
            BTreeChunk c = getChunk(pos);
            long filePos = CHUNK_HEADER_SIZE + DataUtils.getPageOffset(pos);
            if (filePos < 0) {
                throw DataUtils
                        .newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Negative position {0}", filePos);
            }
            long maxPos = c.blockCount * BLOCK_SIZE;
            p = BTreePage.read(c.fileStorage, pos, map, filePos, maxPos);
            cachePage(pos, p, p.getMemory());
        }
        return p;
    }

    /**
     * Remove a page.
     * 
     * @param pos the position of the page
     * @param memory the memory usage
     */
    void removePage(long pos, int memory) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos == 0) {
            return;
        }

        // This could result in a cache miss if the operation is rolled back,
        // but we don't optimize for rollback.
        // We could also keep the page in the cache, as somebody
        // could still read it (reading the old version).
        if (cache != null) {
            if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
                // keep nodes in the cache, because they are still used for
                // garbage collection
                cache.remove(pos);
            }
        }

        BTreeChunk chunk = getChunk(pos);
        long maxLengthLive = DataUtils.getPageMaxLength(pos);

        // synchronize, because pages could be freed concurrently
        synchronized (chunk) {
            chunk.maxLenLive -= maxLengthLive;
            chunk.pageCountLive--;
            chunk.changed = true;
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
     * Get the current version of the data. When a new storage is created, the
     * version is 0.
     * 
     * @return the version
     */
    public long getCurrentVersion() {
        return currentVersion;
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

}
