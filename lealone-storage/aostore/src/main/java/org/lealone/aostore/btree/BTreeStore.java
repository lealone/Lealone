/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.aostore.btree;

import java.io.File;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.aostore.AOStore;
import org.lealone.aostore.FileStore;
import org.lealone.aostore.btree.BTreePage.PageChildren;
import org.lealone.aostore.cache.CacheLongKeyLIRS;
import org.lealone.common.compress.CompressDeflate;
import org.lealone.common.compress.CompressLZF;
import org.lealone.common.compress.Compressor;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.New;
import org.lealone.storage.type.StringDataType;
import org.lealone.storage.type.WriteBuffer;

/*
 * TODO: Documentation - rolling docs review: at "Metadata Map" - better
 * document that writes are in background thread - better document how to do
 * non-unique indexes - document pluggable store and OffHeapStore
 * TransactionStore: - ability to disable the transaction log, if there is only
 * one connection MVStore: - better and clearer memory usage accounting rules
 * (heap memory versus disk memory), so that even there is never an out of
 * memory even for a small heap, and so that chunks are still relatively big on
 * average - make sure serialization / deserialization errors don't corrupt the
 * file - test and possibly improve compact operation (for large dbs) -
 * automated 'kill process' and 'power failure' test - defragment (re-creating
 * maps, specially those with small pages) - store number of write operations
 * per page (maybe defragment if much different than count) - r-tree: nearest
 * neighbor search - use a small object value cache (StringCache), test on
 * Android for default serialization - MVStoreTool.dump should dump the data if
 * possible; possibly using a callback for serialization - implement a sharded
 * map (in one store, multiple stores) to support concurrent updates and writes,
 * and very large maps - to save space when persisting very small transactions,
 * use a transaction log where only the deltas are stored - serialization for
 * lists, sets, sets, sorted sets, maps, sorted maps - maybe rename 'rollback'
 * to 'revert' to distinguish from transactions - support other compression
 * algorithms (deflate, LZ4,...) - remove features that are not really needed;
 * simplify the code possibly using a separate layer or tools (retainVersion?) -
 * optional pluggable checksum mechanism (per page), which requires that
 * everything is a page (including headers) - rename "store" to "save", as
 * "store" is used in "storeVersion" - rename setStoreVersion to setDataVersion,
 * setSchemaVersion or similar - temporary file storage - simple rollback method
 * (rollback to last committed version) - MVMap to implement SortedMap, then
 * NavigableMap - storage that splits database into multiple files, to speed up
 * compact and allow using trim (by truncating / deleting empty files) - add new
 * feature to the file system API to avoid copying data (reads that returns a
 * ByteBuffer instead of writing into one) for memory mapped files and off-heap
 * storage - support log structured merge style operations (blind writes) using
 * one map per level plus bloom filter - have a strict call order MVStore ->
 * MVMap -> Page -> FileStore - autocommit commits, stores, and compacts from
 * time to time; the background thread should wait at least 90% of the
 * configured write delay to store changes - compact* should also store
 * uncommitted changes (if there are any) - write a LSM-tree (log structured
 * merge tree) utility on top of the MVStore with blind writes and/or a bloom
 * filter that internally uses regular maps and merge sort - chunk metadata:
 * maybe split into static and variable, or use a small page size for metadata -
 * data type "string": maybe use prefix compression for keys - test chunk id
 * rollover - feature to auto-compact from time to time and on close - compact
 * very small chunks - Page: to save memory, combine keys & values into one
 * array (also children & counts). Maybe remove some other fields (childrenCount
 * for example) - Support SortedMap for MVMap - compact: copy whole pages
 * (without having to open all maps) - maybe change the length code to have
 * lower gaps - test with very low limits (such as: short chunks, small pages) -
 * maybe allow to read beyond the retention time: when compacting, move live
 * pages in old chunks to a map (possibly the metadata map) - this requires a
 * change in the compaction code, plus a map lookup when reading old data; also,
 * this old data map needs to be cleaned up somehow; maybe using an additional
 * timeout - rollback of removeMap should restore the data - which has big
 * consequences, as the metadata map would probably need references to the root
 * nodes of all maps
 */

/**
 * A persistent storage for map.
 */
public class BTreeStore {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

    /**
     * The block size (physical sector size) of the disk. The store header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    public static final int BLOCK_SIZE = 4 * 1024;

    private static final int FORMAT_WRITE = 1;
    private static final int FORMAT_READ = 1;

    /**
     * Used to mark a chunk as free, when it was detected that live bookkeeping
     * is incorrect.
     */
    private static final int MARKED_FREE = 10000000;

    private volatile boolean reuseSpace = true;

    private boolean closed;

    private FileStore fileStore;
    private boolean fileStoreIsProvided;

    private final int pageSplitSize;

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected
     * number of entries.
     */
    private CacheLongKeyLIRS<BTreePage> cache;

    /**
     * The page chunk references cache. The default size is 4 MB, and the
     * average size is 2 KB. It is split in 16 segments. The stack move distance
     * is 2% of the expected number of entries.
     */
    private CacheLongKeyLIRS<PageChildren> cacheChunkRef;

    /**
     * The newest chunk. If nothing was stored yet, this field is not set.
     */
    private BTreeChunk lastChunk;

    /**
     * The map of chunks.
     */
    private final ConcurrentHashMap<Integer, BTreeChunk> chunks = new ConcurrentHashMap<>();

    /**
     * The map of temporarily freed storage space caused by freed pages. The key
     * is the unsaved version, the value is the map of chunks. The maps contains
     * the number of freed entries per chunk. Access is synchronized.
     */
    private final ConcurrentHashMap<Long, HashMap<Integer, BTreeChunk>> freedPageSpace = new ConcurrentHashMap<>();

    /**
     * The metadata map. Write access to this map needs to be synchronized on
     * the store.
     */
    private BTreeMap<String, String> meta;

    private BTreeMap<Object, Object> map;

    private final HashMap<String, Object> storeHeader = New.hashMap();

    private WriteBuffer writeBuffer;

    private int lastMapId;

    private int versionsToKeep = 5;

    /**
     * The compression level for new pages (0 for disabled, 1 for fast, 2 for
     * high). Even if disabled, the store may contain (old) compressed pages.
     */
    private final int compressionLevel;

    private Compressor compressorFast;

    private Compressor compressorHigh;

    private final UncaughtExceptionHandler backgroundExceptionHandler;

    private long currentVersion;

    /**
     * The version of the last stored chunk, or -1 if nothing was stored so far.
     */
    private long lastStoredVersion;

    /**
     * The estimated memory used by unsaved pages. This number is not accurate,
     * also because it may be changed concurrently, and because temporary pages
     * are counted.
     */
    private int unsavedMemory;
    private int autoCommitMemory;
    private boolean saveNeeded;

    /**
     * The time the store was created, in milliseconds since 1970.
     */
    private long creationTime;

    /**
     * How long to retain old, persisted chunks, in milliseconds. For larger or
     * equal to zero, a chunk is never directly overwritten if unused, but
     * instead, the unused field is set. If smaller zero, chunks are directly
     * overwritten if unused.
     */
    private int retentionTime;

    private long lastCommitTime;

    /**
     * The earliest chunk to retain, if any.
     */
    private BTreeChunk retainChunk;

    /**
     * The version of the current store operation (if any).
     */
    private volatile long currentStoreVersion = -1;

    private Thread currentStoreThread;

    private volatile boolean metaChanged;

    private int autoCompactFillRate;
    private long autoCompactLastFileOpCount;

    private final Object compactSync = new Object();

    private IllegalStateException panicException;

    private long lastTimeAbsolute;

    /**
     * Create and open the store.
     * 
     * @param config the configuration to use
     * @throws IllegalStateException if the file is corrupt, or an exception
     *             occurred while opening
     * @throws IllegalArgumentException if the directory does not exist
     */
    public BTreeStore(String name, HashMap<String, Object> config) {
        Object o = config.get("compress");
        this.compressionLevel = o == null ? 0 : (Integer) o;
        String fileName = (String) config.get("storeName");
        if (fileName != null) {
            fileName = fileName + File.separator + name + AOStore.SUFFIX_AO_FILE;
        }
        o = config.get("pageSplitSize");
        if (o == null) {
            pageSplitSize = fileName == null ? 4 * 1024 : 16 * 1024;
        } else {
            pageSplitSize = (Integer) o;
        }
        o = config.get("backgroundExceptionHandler");
        this.backgroundExceptionHandler = (UncaughtExceptionHandler) o;
        meta = new BTreeMap<String, String>(StringDataType.INSTANCE, StringDataType.INSTANCE);
        HashMap<String, Object> c = New.hashMap();
        c.put("id", 0);
        c.put("createVersion", currentVersion);
        meta.init(this, c);
        fileStore = (FileStore) config.get("fileStore");
        if (fileName == null && fileStore == null) {
            cache = null;
            cacheChunkRef = null;
            return;
        }
        if (fileStore == null) {
            fileStoreIsProvided = false;
            fileStore = new FileStore();
        } else {
            fileStoreIsProvided = true;
        }
        retentionTime = fileStore.getDefaultRetentionTime();
        boolean readOnly = config.containsKey("readOnly");
        o = config.get("cacheSize");
        int mb = o == null ? 16 : (Integer) o;
        if (mb > 0) {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            cc.maxMemory = mb * 1024L * 1024L;
            cache = new CacheLongKeyLIRS<BTreePage>(cc);
            cc.maxMemory /= 4;
            cacheChunkRef = new CacheLongKeyLIRS<PageChildren>(cc);
        }
        o = config.get("autoCommitBufferSize");
        int kb = o == null ? 1024 : (Integer) o;
        // 19 KB memory is about 1 KB storage
        autoCommitMemory = kb * 1024 * 19;

        o = config.get("autoCompactFillRate");
        autoCompactFillRate = o == null ? 50 : (Integer) o;

        char[] encryptionKey = (char[]) config.get("encryptionKey");
        try {
            if (!fileStoreIsProvided) {
                fileStore.open(fileName, readOnly, encryptionKey);
            }
            if (fileStore.size() == 0) {
                creationTime = getTimeAbsolute();
                lastCommitTime = creationTime;
                storeHeader.put("H", 2);
                storeHeader.put("blockSize", BLOCK_SIZE);
                storeHeader.put("format", FORMAT_WRITE);
                storeHeader.put("created", creationTime);
                writeStoreHeader();
            } else {
                readStoreHeader();
            }
        } catch (IllegalStateException e) {
            panic(e);
        } finally {
            if (encryptionKey != null) {
                Arrays.fill(encryptionKey, (char) 0);
            }
        }
        lastCommitTime = getTimeSinceCreation();
    }

    private void panic(IllegalStateException e) {
        if (backgroundExceptionHandler != null) {
            backgroundExceptionHandler.uncaughtException(null, e);
        }
        panicException = e;
        closeImmediately();
        throw e;
    }

    void setMap(BTreeMap<Object, Object> map) {
        // if (this.map != null)
        this.map = map;
    }

    /**
     * Open an old, stored version of a map.
     * 
     * @param version the version
     * @param mapId the map id
     * @param template the template map
     * @return the read-only map
     */

    @SuppressWarnings("unchecked")
    <T extends BTreeMap<?, ?>> T openMapVersion(long version, int mapId, BTreeMap<?, ?> template) {
        BTreeMap<String, String> oldMeta = getMetaMap(version);
        long rootPos = getRootPos(oldMeta, mapId);
        BTreeMap<?, ?> m = template.openReadOnly();
        m.setRootPos(rootPos, version);
        return (T) m;
    }

    /**
     * Open a map with the default settings. The map is automatically create if
     * it does not yet exist. If a map with this name is already open, this map
     * is returned.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @return the map
     */

    public <K, V> BTreeMap<K, V> openMap(String name) {
        return openMap(name, new BTreeMap.Builder<K, V>());
    }

    /**
     * Open a map with the given builder. The map is automatically create if it
     * does not yet exist. If a map with this name is already open, this map is
     * returned.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @param name the name of the map
     * @param builder the map builder
     * @return the map
     */

    @SuppressWarnings("unchecked")
    public synchronized <M extends BTreeMap<K, V>, K, V> M openMap(String name, BTreeMap.MapBuilder<M, K, V> builder) {
        return (M) map;
    }

    /**
     * Get the set of all map names.
     * 
     * @return the set of names
     */

    public synchronized Set<String> getMapNames() {
        HashSet<String> set = New.hashSet();
        checkOpen();
        for (Iterator<String> it = meta.keyIterator("name."); it.hasNext();) {
            String x = it.next();
            if (!x.startsWith("name.")) {
                break;
            }
            set.add(x.substring("name.".length()));
        }
        return set;
    }

    /**
     * Get the metadata map. This data is for informational purposes only. The
     * data is subject to change in future versions.
     * <p>
     * The data in this map should not be modified (changing system data may
     * corrupt the store). If modifications are needed, they need be
     * synchronized on the store.
     * <p>
     * The metadata map contains the following entries:
     * 
     * <pre>
     * chunk.{chunkId} = {chunk metadata}
     * name.{name} = {mapId}
     * map.{mapId} = {map metadata}
     * root.{mapId} = {root position}
     * setting.storeVersion = {version}
     * </pre>
     * 
     * @return the metadata map
     */

    public BTreeMap<String, String> getMetaMap() {
        checkOpen();
        return meta;
    }

    private BTreeMap<String, String> getMetaMap(long version) {
        BTreeChunk c = getChunkForVersion(version);
        DataUtils.checkArgument(c != null, "Unknown version {0}", version);
        c = readChunkHeader(c.block);
        BTreeMap<String, String> oldMeta = meta.openReadOnly();
        oldMeta.setRootPos(c.metaRootPos, version);
        return oldMeta;
    }

    private BTreeChunk getChunkForVersion(long version) {
        BTreeChunk newest = null;
        for (BTreeChunk c : chunks.values()) {
            if (c.version <= version) {
                if (newest == null || c.id > newest.id) {
                    newest = c;
                }
            }
        }
        return newest;
    }

    /**
     * Check whether a given map exists.
     * 
     * @param name the map name
     * @return true if it exists
     */

    public boolean hasMap(String name) {
        return meta.containsKey("name." + name);
    }

    public void markMetaChanged() {
        // changes in the metadata alone are usually not detected, as the meta
        // map is changed after storing
        metaChanged = true;
    }

    private synchronized void readStoreHeader() {
        BTreeChunk newest = null;
        boolean validStoreHeader = false;
        // find out which chunk and version are the newest
        // read the first two blocks
        ByteBuffer fileHeaderBlocks = fileStore.readFully(0, 2 * BLOCK_SIZE);
        byte[] buff = new byte[BLOCK_SIZE];
        for (int i = 0; i <= BLOCK_SIZE; i += BLOCK_SIZE) {
            fileHeaderBlocks.get(buff);
            // the following can fail for various reasons
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
                long version = DataUtils.readHexLong(m, "version", 0);
                if (newest == null || version > newest.version) {
                    validStoreHeader = true;
                    storeHeader.putAll(m);
                    creationTime = DataUtils.readHexLong(m, "created", 0);
                    int chunkId = DataUtils.readHexInt(m, "chunk", 0);
                    long block = DataUtils.readHexLong(m, "block", 0);
                    BTreeChunk test = readChunkHeaderAndFooter(block);
                    if (test != null && test.id == chunkId) {
                        newest = test;
                    }
                }
            } catch (Exception e) {
                continue;
            }
        }
        if (!validStoreHeader) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Store header is corrupt: {0}",
                    fileStore);
        }
        long format = DataUtils.readHexLong(storeHeader, "format", 1);
        if (format > FORMAT_WRITE && !fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The write format {0} is larger " + "than the supported format {1}, "
                            + "and the file was not opened in read-only mode", format, FORMAT_WRITE);
        }
        format = DataUtils.readHexLong(storeHeader, "formatRead", format);
        if (format > FORMAT_READ) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The read format {0} is larger " + "than the supported format {1}", format, FORMAT_READ);
        }
        lastStoredVersion = -1;
        chunks.clear();
        long now = System.currentTimeMillis();
        // calculate the year (doesn't have to be exact;
        // we assume 365.25 days per year, * 4 = 1461)
        int year = 1970 + (int) (now / (1000L * 60 * 60 * 6 * 1461));
        if (year < 2014) {
            // if the year is before 2014,
            // we assume the system doesn't have a real-time clock,
            // and we set the creationTime to the past, so that
            // existing chunks are overwritten
            creationTime = now - fileStore.getDefaultRetentionTime();
        } else if (now < creationTime) {
            // the system time was set to the past:
            // we change the creation time
            creationTime = now;
            storeHeader.put("created", creationTime);
        }
        BTreeChunk test = readChunkFooter(fileStore.size());
        if (test != null) {
            test = readChunkHeaderAndFooter(test.block);
            if (test != null) {
                if (newest == null || test.version > newest.version) {
                    newest = test;
                }
            }
        }
        if (newest == null) {
            // no chunk
            return;
        }
        // read the chunk header and footer,
        // and follow the chain of next chunks
        while (true) {
            if (newest.next == 0 || newest.next >= fileStore.size() / BLOCK_SIZE) {
                // no (valid) next
                break;
            }
            test = readChunkHeaderAndFooter(newest.next);
            if (test == null || test.id <= newest.id) {
                break;
            }
            newest = test;
        }
        setLastChunk(newest);
        loadChunkMeta();
        // read all chunk headers and footers within the retention time,
        // to detect unwritten data after a power failure
        verifyLastChunks();
        // build the free space list
        for (BTreeChunk c : chunks.values()) {
            if (c.pageCountLive == 0) {
                // remove this chunk in the next save operation
                registerFreePage(currentVersion, c.id, 0, 0);
            }
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            fileStore.markUsed(start, length);
        }
    }

    private void loadChunkMeta() {
        // load the chunk metadata: we can load in any order,
        // because loading chunk metadata might recursively load another chunk
        for (Iterator<String> it = meta.keyIterator("chunk."); it.hasNext();) {
            String s = it.next();
            if (!s.startsWith("chunk.")) {
                break;
            }
            s = meta.get(s);
            BTreeChunk c = BTreeChunk.fromString(s);
            if (!chunks.containsKey(c.id)) {
                if (c.block == Long.MAX_VALUE) {
                    throw DataUtils
                            .newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} is invalid", c.id);
                }
                chunks.put(c.id, c);
            }
        }
    }

    private void setLastChunk(BTreeChunk last) {
        lastChunk = last;
        if (last == null) {
            // no valid chunk
            lastMapId = 0;
            currentVersion = 0;
            meta.setRootPos(0, -1);
        } else {
            lastMapId = last.mapId;
            currentVersion = last.version;
            chunks.put(last.id, last);
            meta.setRootPos(last.metaRootPos, -1);
        }
        setWriteVersion(currentVersion);
    }

    private void verifyLastChunks() {
        long time = getTimeSinceCreation();
        ArrayList<Integer> ids = new ArrayList<Integer>(chunks.keySet());
        Collections.sort(ids);
        int newestValidChunk = -1;
        BTreeChunk old = null;
        for (Integer chunkId : ids) {
            BTreeChunk c = chunks.get(chunkId);
            if (old != null && c.time < old.time) {
                // old chunk (maybe leftover from a previous crash)
                break;
            }
            old = c;
            if (c.time + retentionTime < time) {
                // old chunk, no need to verify
                newestValidChunk = c.id;
                continue;
            }
            BTreeChunk test = readChunkHeaderAndFooter(c.block);
            if (test == null || test.id != c.id) {
                break;
            }
            newestValidChunk = chunkId;
        }
        BTreeChunk newest = chunks.get(newestValidChunk);
        if (newest != lastChunk) {
            // to avoid re-using newer chunks later on, we could clear
            // the headers and footers of those, but we might not know about all
            // of them, so that could be incomplete - but we check that newer
            // chunks are written after older chunks, so we are safe
            rollbackTo(newest == null ? 0 : newest.version);
        }
    }

    /**
     * Read a chunk header and footer, and verify the stored data is consistent.
     * 
     * @param block the block
     * @return the chunk, or null if the header or footer don't match or are not
     *         consistent
     */
    private BTreeChunk readChunkHeaderAndFooter(long block) {
        BTreeChunk header;
        try {
            header = readChunkHeader(block);
        } catch (Exception e) {
            // invalid chunk header: ignore, but stop
            return null;
        }
        if (header == null) {
            return null;
        }
        BTreeChunk footer = readChunkFooter((block + header.len) * BLOCK_SIZE);
        if (footer == null || footer.id != header.id) {
            return null;
        }
        return header;
    }

    /**
     * Try to read a chunk footer.
     * 
     * @param end the end of the chunk
     * @return the chunk, or null if not successful
     */
    private BTreeChunk readChunkFooter(long end) {
        // the following can fail for various reasons
        try {
            // read the chunk footer of the last block of the file
            ByteBuffer lastBlock = fileStore.readFully(end - BTreeChunk.FOOTER_LENGTH, BTreeChunk.FOOTER_LENGTH);
            byte[] buff = new byte[BTreeChunk.FOOTER_LENGTH];
            lastBlock.get(buff);
            String s = new String(buff, DataUtils.LATIN).trim();
            HashMap<String, String> m = DataUtils.parseMap(s);
            int check = DataUtils.readHexInt(m, "fletcher", 0);
            m.remove("fletcher");
            s = s.substring(0, s.lastIndexOf("fletcher") - 1);
            byte[] bytes = s.getBytes(DataUtils.LATIN);
            int checksum = DataUtils.getFletcher32(bytes, bytes.length);
            if (check == checksum) {
                int chunk = DataUtils.readHexInt(m, "chunk", 0);
                BTreeChunk c = new BTreeChunk(chunk);
                c.version = DataUtils.readHexLong(m, "version", 0);
                c.block = DataUtils.readHexLong(m, "block", 0);
                return c;
            }
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void writeStoreHeader() {
        StringBuilder buff = new StringBuilder();
        if (lastChunk != null) {
            storeHeader.put("block", lastChunk.block);
            storeHeader.put("chunk", lastChunk.id);
            storeHeader.put("version", lastChunk.version);
        }
        DataUtils.appendMap(buff, storeHeader);
        byte[] bytes = buff.toString().getBytes(DataUtils.LATIN);
        int checksum = DataUtils.getFletcher32(bytes, bytes.length);
        DataUtils.appendMap(buff, "fletcher", checksum);
        buff.append("\n");
        bytes = buff.toString().getBytes(DataUtils.LATIN);
        ByteBuffer header = ByteBuffer.allocate(2 * BLOCK_SIZE);
        header.put(bytes);
        header.position(BLOCK_SIZE);
        header.put(bytes);
        header.rewind();
        write(0, header);
    }

    private void write(long pos, ByteBuffer buffer) {
        try {
            fileStore.writeFully(pos, buffer);
        } catch (IllegalStateException e) {
            panic(e);
            throw e;
        }
    }

    /**
     * Close the file and the store. Unsaved changes are written to disk first.
     */

    public void close() {
        if (closed) {
            return;
        }
        FileStore f = fileStore;
        if (f != null && !f.isReadOnly()) {
            if (hasUnsavedChanges()) {
                commitAndSave();
            }
        }
        closeStore(true);
    }

    /**
     * Close the file and the store, without writing anything. This will stop
     * the background thread. This method ignores all errors.
     */

    public void closeImmediately() {
        try {
            closeStore(false);
        } catch (Exception e) {
            if (backgroundExceptionHandler != null) {
                backgroundExceptionHandler.uncaughtException(null, e);
            }
        }
    }

    private void closeStore(boolean shrinkIfPossible) {
        if (closed) {
            return;
        }
        // // can not synchronize on this yet, because
        // // the thread also synchronized on this, which
        // // could result in a deadlock
        // stopBackgroundThread();
        closed = true;
        if (fileStore == null) {
            return;
        }
        synchronized (this) {
            if (shrinkIfPossible) {
                shrinkFileIfPossible(0);
            }
            // release memory early - this is important when called
            // because of out of memory
            cache = null;
            cacheChunkRef = null;
            map.close();
            meta = null;
            chunks.clear();
            try {
                if (!fileStoreIsProvided) {
                    fileStore.close();
                }
            } finally {
                fileStore = null;
            }
        }
    }

    /**
     * Whether the chunk at the given position is live.
     * 
     * @param the chunk id
     * @return true if it is live
     */

    boolean isChunkLive(int chunkId) {
        String s = meta.get(BTreeChunk.getMetaKey(chunkId));
        return s != null;
    }

    /**
     * Get the chunk for the given position.
     * 
     * @param pos the position
     * @return the chunk
     */
    private BTreeChunk getChunk(long pos) {
        BTreeChunk c = getChunkIfFound(pos);
        if (c == null) {
            int chunkId = DataUtils.getPageChunkId(pos);
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} not found", chunkId);
        }
        return c;
    }

    private BTreeChunk getChunkIfFound(long pos) {
        int chunkId = DataUtils.getPageChunkId(pos);
        BTreeChunk c = chunks.get(chunkId);
        if (c == null) {
            checkOpen();
            if (!Thread.holdsLock(this)) {
                // it could also be unsynchronized metadata
                // access (if synchronization on this was forgotten)
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_CHUNK_NOT_FOUND, "Chunk {0} no longer exists",
                        chunkId);
            }
            String s = meta.get(BTreeChunk.getMetaKey(chunkId));
            if (s == null) {
                return null;
            }
            c = BTreeChunk.fromString(s);
            if (c.block == Long.MAX_VALUE) {
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} is invalid", chunkId);
            }
            chunks.put(c.id, c);
        }
        return c;
    }

    private void setWriteVersion(long version) {
        map.setWriteVersion(version);
        meta.setWriteVersion(version);
    }

    /**
     * Commit the changes.
     * <p>
     * For in-memory stores, this method increments the version.
     * <p>
     * For persistent stores, it also writes changes to disk. It does nothing if
     * there are no unsaved changes, and returns the old version. It is not
     * necessary to call this method when auto-commit is enabled (the default
     * setting), as in this case it is automatically called from time to time or
     * when enough changes have accumulated. However, it may still be called to
     * flush all changes to disk.
     * 
     * @return the new version
     */

    public long commit() {
        if (fileStore != null) {
            return commitAndSave();
        }
        long v = ++currentVersion;
        setWriteVersion(v);
        return v;
    }

    /**
     * Commit all changes and persist them to disk. This method does nothing if
     * there are no unsaved changes, otherwise it increments the current version
     * and stores the data (for file based stores).
     * <p>
     * At most one store operation may run at any time.
     * 
     * @return the new version (incremented if there were changes)
     */
    private synchronized long commitAndSave() {
        if (closed) {
            return currentVersion;
        }
        if (fileStore == null) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "This is an in-memory store");
        }
        if (currentStoreVersion >= 0) {
            // store is possibly called within store, if the meta map changed
            return currentVersion;
        }
        if (!hasUnsavedChanges()) {
            return currentVersion;
        }
        if (fileStore.isReadOnly()) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED, "This store is read-only");
        }
        try {
            currentStoreVersion = currentVersion;
            currentStoreThread = Thread.currentThread();
            return storeNow();
        } finally {
            // in any case reset the current store version,
            // to allow closing the store
            currentStoreVersion = -1;
            currentStoreThread = null;
        }
    }

    private long storeNow() {
        try {
            return storeNowTry();
        } catch (IllegalStateException e) {
            panic(e);
            return -1;
        }
    }

    private long storeNowTry() {
        freeUnusedChunks();

        int currentUnsavedPageCount = unsavedMemory;
        long storeVersion = currentStoreVersion;
        long version = ++currentVersion;
        setWriteVersion(version);
        long time = getTimeSinceCreation();
        lastCommitTime = time;
        retainChunk = null;

        // the metadata of the last chunk was not stored so far, and needs to be
        // set now (it's better not to update right after storing, because that
        // would modify the meta map again)
        int lastChunkId;
        if (lastChunk == null) {
            lastChunkId = 0;
        } else {
            lastChunkId = lastChunk.id;
            meta.put(BTreeChunk.getMetaKey(lastChunkId), lastChunk.asString());
            // never go backward in time
            time = Math.max(lastChunk.time, time);
        }
        int newChunkId = lastChunkId;
        while (true) {
            newChunkId = (newChunkId + 1) % BTreeChunk.MAX_ID;
            BTreeChunk old = chunks.get(newChunkId);
            if (old == null) {
                break;
            }
            if (old.block == Long.MAX_VALUE) {
                IllegalStateException e = DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                        "Last block not stored, possibly due to out-of-memory");
                panic(e);
            }
        }
        BTreeChunk c = new BTreeChunk(newChunkId);

        c.pageCount = Integer.MAX_VALUE;
        c.pageCountLive = Integer.MAX_VALUE;
        c.maxLen = Long.MAX_VALUE;
        c.maxLenLive = Long.MAX_VALUE;
        c.metaRootPos = Long.MAX_VALUE;
        c.block = Long.MAX_VALUE;
        c.len = Integer.MAX_VALUE;
        c.time = time;
        c.version = version;
        c.mapId = lastMapId;
        c.next = Long.MAX_VALUE;
        chunks.put(c.id, c);
        // force a metadata update
        meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
        meta.remove(BTreeChunk.getMetaKey(c.id));
        ArrayList<BTreeMap<?, ?>> list = New.arrayList(Collections.<BTreeMap<?, ?>> singleton(map));
        ArrayList<BTreeMap<?, ?>> changed = New.arrayList();
        for (BTreeMap<?, ?> m : list) {
            m.setWriteVersion(version);
            long v = m.getVersion();
            if (m.getCreateVersion() > storeVersion) {
                // the map was created after storing started
                continue;
            }
            if (m.isVolatile()) {
                continue;
            }
            if (v >= 0 && v >= lastStoredVersion) {
                BTreeMap<?, ?> r = m.openVersion(storeVersion);
                if (r.getRoot().getPos() == 0) {
                    changed.add(r);
                }
            }
        }
        applyFreedSpace(storeVersion);
        WriteBuffer buff = getWriteBuffer();
        // need to patch the header later
        c.writeChunkHeader(buff, 0);
        int headerLength = buff.position();
        c.pageCount = 0;
        c.pageCountLive = 0;
        c.maxLen = 0;
        c.maxLenLive = 0;
        for (BTreeMap<?, ?> m : changed) {
            BTreePage p = m.getRoot();
            String key = BTreeMap.getMapRootKey(m.getId());
            if (p.getTotalCount() == 0) {
                meta.put(key, "0");
            } else {
                p.writeUnsavedRecursive(c, buff);
                long root = p.getPos();
                meta.put(key, Long.toHexString(root));
            }
        }
        meta.setWriteVersion(version);

        BTreePage metaRoot = meta.getRoot();
        metaRoot.writeUnsavedRecursive(c, buff);

        int chunkLength = buff.position();

        // add the store header and round to the next block
        int length = MathUtils.roundUpInt(chunkLength + BTreeChunk.FOOTER_LENGTH, BLOCK_SIZE);
        buff.limit(length);

        // the length of the file that is still in use
        // (not necessarily the end of the file)
        long end = getFileLengthInUse();
        long filePos;
        if (reuseSpace) {
            filePos = fileStore.allocate(length);
        } else {
            filePos = end;
        }
        // end is not necessarily the end of the file
        boolean storeAtEndOfFile = filePos + length >= fileStore.size();

        if (!reuseSpace) {
            // we can not mark it earlier, because it
            // might have been allocated by one of the
            // removed chunks
            fileStore.markUsed(end, length);
        }

        c.block = filePos / BLOCK_SIZE;
        c.len = length / BLOCK_SIZE;
        c.metaRootPos = metaRoot.getPos();
        // calculate and set the likely next position
        if (reuseSpace) {
            int predictBlocks = c.len;
            long predictedNextStart = fileStore.allocate(predictBlocks * BLOCK_SIZE);
            fileStore.free(predictedNextStart, predictBlocks * BLOCK_SIZE);
            c.next = predictedNextStart / BLOCK_SIZE;
        } else {
            // just after this chunk
            c.next = 0;
        }
        buff.position(0);
        c.writeChunkHeader(buff, headerLength);
        revertTemp(storeVersion);

        buff.position(buff.limit() - BTreeChunk.FOOTER_LENGTH);
        buff.put(c.getFooterBytes());

        buff.position(0);
        write(filePos, buff.getBuffer());
        releaseWriteBuffer(buff);

        // whether we need to write the store header
        boolean writeStoreHeader = false;
        if (!storeAtEndOfFile) {
            if (lastChunk == null) {
                writeStoreHeader = true;
            } else if (lastChunk.next != c.block) {
                // the last prediction did not matched
                writeStoreHeader = true;
            } else {
                long headerVersion = DataUtils.readHexLong(storeHeader, "version", 0);
                if (lastChunk.version - headerVersion > 20) {
                    // we write after at least 20 entries
                    writeStoreHeader = true;
                } else {
                    int chunkId = DataUtils.readHexInt(storeHeader, "chunk", 0);
                    while (true) {
                        BTreeChunk old = chunks.get(chunkId);
                        if (old == null) {
                            // one of the chunks in between
                            // was removed
                            writeStoreHeader = true;
                            break;
                        }
                        if (chunkId == lastChunk.id) {
                            break;
                        }
                        chunkId++;
                    }
                }
            }
        }

        lastChunk = c;
        if (writeStoreHeader) {
            writeStoreHeader();
        }
        if (!storeAtEndOfFile) {
            // may only shrink after the store header was written
            shrinkFileIfPossible(1);
        }
        for (BTreeMap<?, ?> m : changed) {
            BTreePage p = m.getRoot();
            if (p.getTotalCount() > 0) {
                p.writeEnd();
            }
        }
        metaRoot.writeEnd();

        // some pages might have been changed in the meantime (in the newest
        // version)
        unsavedMemory = Math.max(0, unsavedMemory - currentUnsavedPageCount);

        metaChanged = false;
        lastStoredVersion = storeVersion;

        return version;
    }

    private synchronized void freeUnusedChunks() {
        if (lastChunk == null || !reuseSpace) {
            return;
        }
        Set<Integer> referenced = collectReferencedChunks();
        ArrayList<BTreeChunk> free = New.arrayList();
        long time = getTimeSinceCreation();
        for (BTreeChunk c : chunks.values()) {
            if (!referenced.contains(c.id)) {
                free.add(c);
            }
        }
        for (BTreeChunk c : free) {
            if (canOverwriteChunk(c, time)) {
                chunks.remove(c.id);
                markMetaChanged();
                meta.remove(BTreeChunk.getMetaKey(c.id));
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.free(start, length);
            } else {
                if (c.unused == 0) {
                    c.unused = time;
                    meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
                    markMetaChanged();
                }
            }
        }
    }

    private Set<Integer> collectReferencedChunks() {
        long testVersion = lastChunk.version;
        DataUtils.checkArgument(testVersion > 0, "Collect references on version 0");
        long readCount = getFileStore().getReadCount();
        Set<Integer> referenced = New.hashSet();
        for (org.lealone.storage.StorageMap.Cursor<String, String> c = meta.cursor("root."); c.hasNext();) {
            String key = c.next();
            if (!key.startsWith("root.")) {
                break;
            }
            long pos = DataUtils.parseHexLong(c.getValue());
            if (pos == 0) {
                continue;
            }
            int mapId = DataUtils.parseHexInt(key.substring("root.".length()));
            collectReferencedChunks(referenced, mapId, pos, 0);
        }
        long pos = lastChunk.metaRootPos;
        collectReferencedChunks(referenced, 0, pos, 0);
        readCount = fileStore.getReadCount() - readCount;
        return referenced;
    }

    private void collectReferencedChunks(Set<Integer> targetChunkSet, int mapId, long pos, int level) {
        int c = DataUtils.getPageChunkId(pos);
        targetChunkSet.add(c);
        if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
            return;
        }
        PageChildren refs = readPageChunkReferences(mapId, pos, -1);
        if (!refs.chunkList) {
            Set<Integer> target = New.hashSet();
            for (int i = 0; i < refs.children.length; i++) {
                long p = refs.children[i];
                collectReferencedChunks(target, mapId, p, level + 1);
            }
            // we don't need a reference to this chunk
            target.remove(c);
            long[] children = new long[target.size()];
            int i = 0;
            for (Integer p : target) {
                children[i++] = DataUtils.getPagePos(p, 0, 0, DataUtils.PAGE_TYPE_LEAF);
            }
            refs.children = children;
            refs.chunkList = true;
            if (cacheChunkRef != null) {
                cacheChunkRef.put(refs.pos, refs, refs.getMemory());
            }
        }
        for (long p : refs.children) {
            targetChunkSet.add(DataUtils.getPageChunkId(p));
        }
    }

    private PageChildren readPageChunkReferences(int mapId, long pos, int parentChunk) {
        if (DataUtils.getPageType(pos) == DataUtils.PAGE_TYPE_LEAF) {
            return null;
        }
        PageChildren r;
        if (cacheChunkRef != null) {
            r = cacheChunkRef.get(pos);
        } else {
            r = null;
        }
        if (r == null) {
            // if possible, create it from the cached page
            if (cache != null) {
                BTreePage p = cache.get(pos);
                if (p != null) {
                    r = new PageChildren(p);
                }
            }
            if (r == null) {
                // page was not cached: read the data
                BTreeChunk c = getChunk(pos);
                long filePos = c.block * BLOCK_SIZE;
                filePos += DataUtils.getPageOffset(pos);
                if (filePos < 0) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                            "Negative position {0}; p={1}, c={2}", filePos, pos, c.toString());
                }
                long maxPos = (c.block + c.len) * BLOCK_SIZE;
                r = PageChildren.read(fileStore, pos, mapId, filePos, maxPos);
            }
            r.removeDuplicateChunkReferences();
            if (cacheChunkRef != null) {
                cacheChunkRef.put(pos, r, r.getMemory());
            }
        }
        if (r.children.length == 0) {
            int chunk = DataUtils.getPageChunkId(pos);
            if (chunk == parentChunk) {
                return null;
            }
        }
        return r;
    }

    /**
     * Get a buffer for writing. This caller must synchronize on the store
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
     * Release a buffer for writing. This caller must synchronize on the store
     * before calling the method and until after using the buffer.
     * 
     * @param buff the buffer than can be re-used
     */
    private void releaseWriteBuffer(WriteBuffer buff) {
        if (buff.capacity() <= 4 * 1024 * 1024) {
            writeBuffer = buff;
        }
    }

    private boolean canOverwriteChunk(BTreeChunk c, long time) {
        if (retentionTime >= 0) {
            if (c.time + retentionTime > time) {
                return false;
            }
            if (c.unused == 0 || c.unused + retentionTime / 2 > time) {
                return false;
            }
        }
        BTreeChunk r = retainChunk;
        if (r != null && c.version > r.version) {
            return false;
        }
        return true;
    }

    private long getTimeSinceCreation() {
        return Math.max(0, getTimeAbsolute() - creationTime);
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

    /**
     * Apply the freed space to the chunk metadata. The metadata is updated, but
     * completely free chunks are not removed from the set of chunks, and the
     * disk space is not yet marked as free.
     * 
     * @param storeVersion apply up to the given version
     */
    private void applyFreedSpace(long storeVersion) {
        while (true) {
            ArrayList<BTreeChunk> modified = New.arrayList();
            Iterator<Entry<Long, HashMap<Integer, BTreeChunk>>> it;
            it = freedPageSpace.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Long, HashMap<Integer, BTreeChunk>> e = it.next();
                long v = e.getKey();
                if (v > storeVersion) {
                    continue;
                }
                HashMap<Integer, BTreeChunk> freed = e.getValue();
                for (BTreeChunk f : freed.values()) {
                    BTreeChunk c = chunks.get(f.id);
                    if (c == null) {
                        // already removed
                        continue;
                    }
                    // no need to synchronize, as old entries
                    // are not concurrently modified
                    c.maxLenLive += f.maxLenLive;
                    c.pageCountLive += f.pageCountLive;
                    if (c.pageCountLive < 0 && c.pageCountLive > -MARKED_FREE) {
                        // can happen after a rollback
                        c.pageCountLive = 0;
                    }
                    if (c.maxLenLive < 0 && c.maxLenLive > -MARKED_FREE) {
                        // can happen after a rollback
                        c.maxLenLive = 0;
                    }
                    modified.add(c);
                }
                it.remove();
            }
            for (BTreeChunk c : modified) {
                meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
            }
            if (modified.size() == 0) {
                break;
            }
        }
    }

    /**
     * Shrink the file if possible, and if at least a given percentage can be
     * saved.
     * 
     * @param minPercent the minimum percentage to save
     */
    private void shrinkFileIfPossible(int minPercent) {
        long end = getFileLengthInUse();
        long fileSize = fileStore.size();
        if (end >= fileSize) {
            return;
        }
        if (minPercent > 0 && fileSize - end < BLOCK_SIZE) {
            return;
        }
        int savedPercent = (int) (100 - (end * 100 / fileSize));
        if (savedPercent < minPercent) {
            return;
        }
        if (!closed) {
            sync();
        }
        fileStore.truncate(end);
    }

    /**
     * Get the position of the last used byte.
     * 
     * @return the position
     */
    private long getFileLengthInUse() {
        long size = 2 * BLOCK_SIZE;
        for (BTreeChunk c : chunks.values()) {
            if (c.len != Integer.MAX_VALUE) {
                long x = (c.block + c.len) * BLOCK_SIZE;
                size = Math.max(size, x);
            }
        }
        return size;
    }

    /**
     * Check whether there are any unsaved changes.
     * 
     * @return if there are any changes
     */

    public boolean hasUnsavedChanges() {
        checkOpen();
        if (metaChanged) {
            return true;
        }
        if (!map.isClosed()) {
            long v = map.getVersion();
            if (v >= 0 && v > lastStoredVersion) {
                return true;
            }
        }
        return false;
    }

    private BTreeChunk readChunkHeader(long block) {
        long p = block * BLOCK_SIZE;
        ByteBuffer buff = fileStore.readFully(p, BTreeChunk.MAX_HEADER_LENGTH);
        return BTreeChunk.readChunkHeader(buff, p);
    }

    /**
     * Compact the store by moving all live pages to new chunks.
     * 
     * @return if anything was written
     */

    public synchronized boolean compactRewriteFully() {
        checkOpen();
        if (lastChunk == null) {
            // nothing to do
            return false;
        }
        Cursor<?, ?> cursor = (Cursor<?, ?>) map.cursor(null);
        BTreePage lastPage = null;
        while (cursor.hasNext()) {
            cursor.next();
            BTreePage p = cursor.getPage();
            if (p == lastPage) {
                continue;
            }
            Object k = p.getKey(0);
            Object v = p.getValue(0);
            map.put(k, v);
            lastPage = p;
        }
        commitAndSave();
        return true;
    }

    /**
     * Compact by moving all chunks next to each other.
     * 
     * @return if anything was written
     */

    public synchronized boolean compactMoveChunks() {
        return compactMoveChunks(100, Long.MAX_VALUE);
    }

    /**
     * Compact the store by moving all chunks next to each other, if there is
     * free space between chunks. This might temporarily increase the file size.
     * Chunks are overwritten irrespective of the current retention time. Before
     * overwriting chunks and before resizing the file, syncFile() is called.
     * 
     * @param targetFillRate do nothing if the file store fill rate is higher
     *            than this
     * @param moveSize the number of bytes to move
     * @return if anything was written
     */

    public synchronized boolean compactMoveChunks(int targetFillRate, long moveSize) {
        checkOpen();
        if (lastChunk == null || !reuseSpace) {
            // nothing to do
            return false;
        }
        int oldRetentionTime = retentionTime;
        boolean oldReuse = reuseSpace;
        try {
            retentionTime = -1;
            freeUnusedChunks();
            if (fileStore.getFillRate() > targetFillRate) {
                return false;
            }
            long start = fileStore.getFirstFree() / BLOCK_SIZE;
            ArrayList<BTreeChunk> move = compactGetMoveBlocks(start, moveSize);
            compactMoveChunks(move);
            freeUnusedChunks();
            storeNow();
        } finally {
            reuseSpace = oldReuse;
            retentionTime = oldRetentionTime;
        }
        return true;
    }

    private ArrayList<BTreeChunk> compactGetMoveBlocks(long startBlock, long moveSize) {
        ArrayList<BTreeChunk> move = New.arrayList();
        for (BTreeChunk c : chunks.values()) {
            if (c.block > startBlock) {
                move.add(c);
            }
        }
        // sort by block
        Collections.sort(move, new Comparator<BTreeChunk>() {

            @Override
            public int compare(BTreeChunk o1, BTreeChunk o2) {
                return Long.signum(o1.block - o2.block);
            }
        });
        // find which is the last block to keep
        int count = 0;
        long size = 0;
        for (BTreeChunk c : move) {
            long chunkSize = c.len * (long) BLOCK_SIZE;
            if (size + chunkSize > moveSize) {
                break;
            }
            size += chunkSize;
            count++;
        }
        // move the first block (so the first gap is moved),
        // and the one at the end (so the file shrinks)
        while (move.size() > count && move.size() > 1) {
            move.remove(1);
        }

        return move;
    }

    private void compactMoveChunks(ArrayList<BTreeChunk> move) {
        for (BTreeChunk c : move) {
            WriteBuffer buff = getWriteBuffer();
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            BTreeChunk.readChunkHeader(readBuff, start);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long end = getFileLengthInUse();
            fileStore.markUsed(end, length);
            fileStore.free(start, length);
            c.block = end / BLOCK_SIZE;
            c.next = 0;
            buff.position(0);
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - BTreeChunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());
            buff.position(0);
            write(end, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
        }

        // update the metadata (store at the end of the file)
        reuseSpace = false;
        commitAndSave();
        sync();

        // now re-use the empty space
        reuseSpace = true;
        for (BTreeChunk c : move) {
            if (!chunks.containsKey(c.id)) {
                // already removed during the
                // previous store operation
                continue;
            }
            WriteBuffer buff = getWriteBuffer();
            long start = c.block * BLOCK_SIZE;
            int length = c.len * BLOCK_SIZE;
            buff.limit(length);
            ByteBuffer readBuff = fileStore.readFully(start, length);
            BTreeChunk.readChunkHeader(readBuff, 0);
            int chunkHeaderLen = readBuff.position();
            buff.position(chunkHeaderLen);
            buff.put(readBuff);
            long pos = fileStore.allocate(length);
            fileStore.free(start, length);
            buff.position(0);
            c.block = pos / BLOCK_SIZE;
            c.writeChunkHeader(buff, chunkHeaderLen);
            buff.position(length - BTreeChunk.FOOTER_LENGTH);
            buff.put(c.getFooterBytes());
            buff.position(0);
            write(pos, buff.getBuffer());
            releaseWriteBuffer(buff);
            markMetaChanged();
            meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
        }

        // update the metadata (within the file)
        commitAndSave();
        sync();
        shrinkFileIfPossible(0);
    }

    /**
     * Force all stored changes to be written to the storage. The default
     * implementation calls FileChannel.force(true).
     */

    public void sync() {
        checkOpen();
        FileStore f = fileStore;
        if (f != null) {
            f.sync();
        }
    }

    /**
     * Try to increase the fill rate by re-writing partially full chunks. Chunks
     * with a low number of live items are re-written.
     * <p>
     * If the current fill rate is higher than the target fill rate, nothing is
     * done.
     * <p>
     * Please note this method will not necessarily reduce the file size, as
     * empty chunks are not overwritten.
     * <p>
     * Only data of open maps can be moved. For maps that are not open, the old
     * chunk is still referenced. Therefore, it is recommended to open all maps
     * before calling this method.
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
            if (old == null || old.size() == 0) {
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
        HashSet<Integer> set = New.hashSet();
        for (BTreeChunk c : old) {
            set.add(c.id);
        }
        if (!map.rewrite(set)) {
            return;
        }
        if (!meta.rewrite(set)) {
            return;
        }
        freeUnusedChunks();
        commitAndSave();
    }

    /**
     * Read a page.
     * 
     * @param map the map
     * @param pos the page position
     * @return the page
     */

    BTreePage readPage(BTreeMap<?, ?> map, long pos) {
        if (pos == 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Position 0");
        }
        BTreePage p = cache == null ? null : cache.get(pos);
        if (p == null) {
            BTreeChunk c = getChunk(pos);
            long filePos = c.block * BLOCK_SIZE;
            filePos += DataUtils.getPageOffset(pos);
            if (filePos < 0) {
                throw DataUtils
                        .newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Negative position {0}", filePos);
            }
            long maxPos = (c.block + c.len) * BLOCK_SIZE;
            p = BTreePage.read(fileStore, pos, map, filePos, maxPos);
            cachePage(pos, p, p.getMemory());
        }
        return p;
    }

    /**
     * Remove a page.
     * 
     * @param map the map the page belongs to
     * @param pos the position of the page
     * @param memory the memory usage
     */

    void removePage(BTreeMap<?, ?> map, long pos, int memory) {
        // we need to keep temporary pages,
        // to support reading old versions and rollback
        if (pos == 0) {
            // the page was not yet stored:
            // just using "unsavedMemory -= memory" could result in negative
            // values, because in some cases a page is allocated, but never
            // stored, so we need to use max
            unsavedMemory = Math.max(0, unsavedMemory - memory);
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

        BTreeChunk c = getChunk(pos);
        long version = currentVersion;
        if (map == meta && currentStoreVersion >= 0) {
            if (Thread.currentThread() == currentStoreThread) {
                // if the meta map is modified while storing,
                // then this freed page needs to be registered
                // with the stored chunk, so that the old chunk
                // can be re-used
                version = currentStoreVersion;
            }
        }
        registerFreePage(version, c.id, DataUtils.getPageMaxLength(pos), 1);
    }

    private void registerFreePage(long version, int chunkId, long maxLengthLive, int pageCount) {
        HashMap<Integer, BTreeChunk> freed = freedPageSpace.get(version);
        if (freed == null) {
            freed = New.hashMap();
            HashMap<Integer, BTreeChunk> f2 = freedPageSpace.putIfAbsent(version, freed);
            if (f2 != null) {
                freed = f2;
            }
        }
        // synchronize, because pages could be freed concurrently
        synchronized (freed) {
            BTreeChunk f = freed.get(chunkId);
            if (f == null) {
                f = new BTreeChunk(chunkId);
                freed.put(chunkId, f);
            }
            f.maxLenLive -= maxLengthLive;
            f.pageCountLive -= pageCount;
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

    public boolean getReuseSpace() {
        return reuseSpace;
    }

    /**
     * Whether empty space in the file should be re-used. If enabled, old data
     * is overwritten (default). If disabled, writes are appended at the end of
     * the file.
     * <p>
     * This setting is specially useful for online backup. To create an online
     * backup, disable this setting, then copy the file (starting at the
     * beginning of the file). In this case, concurrent backup and write
     * operations are possible (obviously the backup process needs to be faster
     * than the write operations).
     * 
     * @param reuseSpace the new value
     */

    public void setReuseSpace(boolean reuseSpace) {
        this.reuseSpace = reuseSpace;
    }

    public int getRetentionTime() {
        return retentionTime;
    }

    /**
     * How long to retain old, persisted chunks, in milliseconds. Chunks that
     * are older may be overwritten once they contain no live data.
     * <p>
     * The default value is 45000 (45 seconds) when using the default file
     * store. It is assumed that a file system and hard disk will flush all
     * write buffers within this time. Using a lower value might be dangerous,
     * unless the file system and hard disk flush the buffers earlier. To
     * manually flush the buffers, use
     * <code>MVStore.getFile().force(true)</code>, however please note that
     * according to various tests this does not always work as expected
     * depending on the operating system and hardware.
     * <p>
     * The retention time needs to be long enough to allow reading old chunks
     * while traversing over the entries of a map.
     * <p>
     * This setting is not persisted.
     * 
     * @param ms how many milliseconds to retain old chunks (0 to overwrite them
     *            as early as possible)
     */

    public void setRetentionTime(int ms) {
        this.retentionTime = ms;
    }

    /**
     * How many versions to retain for in-memory stores. If not set, 5 old
     * versions are retained.
     * 
     * @param count the number of versions to keep
     */

    public void setVersionsToKeep(int count) {
        this.versionsToKeep = count;
    }

    /**
     * Get the oldest version to retain in memory (for in-memory stores).
     * 
     * @return the version
     */

    public long getVersionsToKeep() {
        return versionsToKeep;
    }

    /**
     * Get the oldest version to retain in memory, which is the manually set
     * retain version, or the current store version (whatever is older).
     * 
     * @return the version
     */

    long getOldestVersionToKeep() {
        long v = currentVersion;
        if (fileStore == null) {
            return v - versionsToKeep;
        }
        long storeVersion = currentStoreVersion;
        if (storeVersion > -1) {
            v = Math.min(v, storeVersion);
        }
        return v;
    }

    /**
     * Check whether all data can be read from this version. This requires that
     * all chunks referenced by this version are still available (not
     * overwritten).
     * 
     * @param version the version
     * @return true if all data can be read
     */
    private boolean isKnownVersion(long version) {
        if (version > currentVersion || version < 0) {
            return false;
        }
        if (version == currentVersion || chunks.size() == 0) {
            // no stored data
            return true;
        }
        // need to check if a chunk for this version exists
        BTreeChunk c = getChunkForVersion(version);
        if (c == null) {
            return false;
        }
        // also, all chunks referenced by this version
        // need to be available in the file
        BTreeMap<String, String> oldMeta = getMetaMap(version);
        if (oldMeta == null) {
            return false;
        }
        try {
            for (Iterator<String> it = oldMeta.keyIterator("chunk."); it.hasNext();) {
                String chunkKey = it.next();
                if (!chunkKey.startsWith("chunk.")) {
                    break;
                }
                if (!meta.containsKey(chunkKey)) {
                    String s = oldMeta.get(chunkKey);
                    BTreeChunk c2 = BTreeChunk.fromString(s);
                    BTreeChunk test = readChunkHeaderAndFooter(c2.block);
                    if (test == null || test.id != c2.id) {
                        return false;
                    }
                    // we store this chunk
                    chunks.put(c2.id, c2);
                }
            }
        } catch (IllegalStateException e) {
            // the chunk missing where the metadata is stored
            return false;
        }
        return true;
    }

    /**
     * Increment the number of unsaved pages.
     * 
     * @param memory the memory usage of the page
     */

    void registerUnsavedPage(int memory) {
        unsavedMemory += memory;
        int newValue = unsavedMemory;
        if (newValue > autoCommitMemory && autoCommitMemory > 0) {
            saveNeeded = true;
        }
    }

    /**
     * This method is called before writing to a map.
     * 
     * @param map the map
     */

    void beforeWrite(BTreeMap<?, ?> map) {
        if (saveNeeded) {
            if (map == meta) {
                // to, don't save while the metadata map is locked
                // this is to avoid deadlocks that could occur when we
                // synchronize on the store and then on the metadata map
                // TODO there should be no deadlocks possible
                return;
            }
            saveNeeded = false;
            // check again, because it could have been written by now
            if (unsavedMemory > autoCommitMemory && autoCommitMemory > 0) {
                commitAndSave();
            }
        }
    }

    /**
     * Get the store version. The store version is usually used to upgrade the
     * structure of the store after upgrading the application. Initially the
     * store version is 0, until it is changed.
     * 
     * @return the store version
     */

    public int getStoreVersion() {
        checkOpen();
        String x = meta.get("setting.storeVersion");
        return x == null ? 0 : DataUtils.parseHexInt(x);
    }

    /**
     * Update the store version.
     * 
     * @param version the new store version
     */

    public synchronized void setStoreVersion(int version) {
        checkOpen();
        markMetaChanged();
        meta.put("setting.storeVersion", Integer.toHexString(version));
    }

    /**
     * Revert to the beginning of the current version, reverting all uncommitted
     * changes.
     */

    public void rollback() {
        rollbackTo(currentVersion);
    }

    /**
     * Revert to the beginning of the given version. All later changes (stored
     * or not) are forgotten. All maps that were created later are closed. A
     * rollback to a version before the last stored version is immediately
     * persisted. Rollback to version 0 means all data is removed.
     * 
     * @param version the version to revert to
     */

    public synchronized void rollbackTo(long version) {
        checkOpen();
        if (version == 0) {
            // special case: remove all data
            map.close();
            meta.clear();
            chunks.clear();
            if (fileStore != null) {
                fileStore.clear();
            }
            freedPageSpace.clear();
            currentVersion = version;
            setWriteVersion(version);
            metaChanged = false;
            return;
        }
        DataUtils.checkArgument(isKnownVersion(version), "Unknown version {0}", version);
        map.rollbackTo(version);
        for (long v = currentVersion; v >= version; v--) {
            if (freedPageSpace.size() == 0) {
                break;
            }
            freedPageSpace.remove(v);
        }
        meta.rollbackTo(version);
        metaChanged = false;
        boolean loadFromFile = false;
        // find out which chunks to remove,
        // and which is the newest chunk to keep
        // (the chunk list can have gaps)
        ArrayList<Integer> remove = new ArrayList<Integer>();
        BTreeChunk keep = null;
        for (BTreeChunk c : chunks.values()) {
            if (c.version > version) {
                remove.add(c.id);
            } else if (keep == null || keep.id < c.id) {
                keep = c;
            }
        }
        if (remove.size() > 0) {
            // remove the youngest first, so we don't create gaps
            // (in case we remove many chunks)
            Collections.sort(remove, Collections.reverseOrder());
            revertTemp(version);
            loadFromFile = true;
            for (int id : remove) {
                BTreeChunk c = chunks.remove(id);
                long start = c.block * BLOCK_SIZE;
                int length = c.len * BLOCK_SIZE;
                fileStore.free(start, length);
                // overwrite the chunk,
                // so it is not be used later on
                WriteBuffer buff = getWriteBuffer();
                buff.limit(length);
                // buff.clear() does not set the data
                Arrays.fill(buff.getBuffer().array(), (byte) 0);
                write(start, buff.getBuffer());
                releaseWriteBuffer(buff);
                // only really needed if we remove many chunks, when writes are
                // re-ordered - but we do it always, because rollback is not
                // performance critical
                sync();
            }
            lastChunk = keep;
            writeStoreHeader();
            readStoreHeader();
        }
        int id = map.getId();
        if (map.getCreateVersion() >= version) {
            map.close();
        } else {
            if (loadFromFile) {
                map.setRootPos(getRootPos(meta, id), -1);
            }
        }
        // rollback might have rolled back the stored chunk metadata as well
        if (lastChunk != null) {
            for (BTreeChunk c : chunks.values()) {
                meta.put(BTreeChunk.getMetaKey(c.id), c.asString());
            }
        }
        currentVersion = version;
        setWriteVersion(version);
    }

    private static long getRootPos(BTreeMap<String, String> map, int mapId) {
        String root = map.get(BTreeMap.getMapRootKey(mapId));
        return root == null ? 0 : DataUtils.parseHexLong(root);
    }

    private void revertTemp(long storeVersion) {
        for (Iterator<Long> it = freedPageSpace.keySet().iterator(); it.hasNext();) {
            long v = it.next();
            if (v > storeVersion) {
                continue;
            }
            it.remove();
        }
        map.removeUnusedOldVersions();
    }

    /**
     * Get the current version of the data. When a new store is created, the
     * version is 0.
     * 
     * @return the version
     */

    public long getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Get the file store.
     * 
     * @return the file store
     */

    public FileStore getFileStore() {
        return fileStore;
    }

    /**
     * Get the store header. This data is for informational purposes only. The
     * data is subject to change in future versions. The data should not be
     * modified (doing so may corrupt the store).
     * 
     * @return the store header
     */

    public Map<String, Object> getStoreHeader() {
        return storeHeader;
    }

    private void checkOpen() {
        if (closed) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_CLOSED, "This store is closed", panicException);
        }
    }

    /**
     * Rename a map.
     * 
     * @param map the map
     * @param newName the new name
     */

    public synchronized void renameMap(BTreeMap<?, ?> map, String newName) {
        checkOpen();
        DataUtils.checkArgument(map != meta, "Renaming the meta map is not allowed");
        int id = map.getId();
        String oldName = getMapName(id);
        if (oldName.equals(newName)) {
            return;
        }
        DataUtils.checkArgument(!meta.containsKey("name." + newName), "A map named {0} already exists", newName);
        markMetaChanged();
        String x = Integer.toHexString(id);
        meta.remove("name." + oldName);
        meta.put(BTreeMap.getMapKey(id), map.asString(newName));
        meta.put("name." + newName, x);
    }

    /**
     * Remove a map. Please note rolling back this operation does not restore
     * the data; if you need this ability, use Map.clear().
     * 
     * @param map the map to remove
     */

    public synchronized void removeMap(BTreeMap<?, ?> map) {
        checkOpen();
        DataUtils.checkArgument(map != meta, "Removing the meta map is not allowed");
        map.clear();
        int id = map.getId();
        String name = getMapName(id);
        markMetaChanged();
        meta.remove(BTreeMap.getMapKey(id));
        meta.remove("name." + name);
        meta.remove(BTreeMap.getMapRootKey(id));
    }

    /**
     * Get the name of the given map.
     * 
     * @param id the map id
     * @return the name, or null if not found
     */

    public synchronized String getMapName(int id) {
        checkOpen();
        String m = meta.get(BTreeMap.getMapKey(id));
        return m == null ? null : DataUtils.parseMap(m).get("name");
    }

    /**
     * Commit and save all changes, if there are any, and compact the store if
     * needed.
     */

    public void writeInBackground(int autoCommitDelay) {
        if (closed) {
            return;
        }

        // could also commit when there are many unsaved pages,
        // but according to a test it doesn't really help
        long time = getTimeSinceCreation();
        if (time <= lastCommitTime + autoCommitDelay) {
            return;
        }
        if (hasUnsavedChanges()) {
            try {
                commitAndSave();
            } catch (Exception e) {
                if (backgroundExceptionHandler != null) {
                    backgroundExceptionHandler.uncaughtException(null, e);
                    return;
                }
            }
        }
        if (autoCompactFillRate > 0) {
            try {
                // whether there were file read or write operations since
                // the last time
                boolean fileOps;
                long fileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
                if (autoCompactLastFileOpCount != fileOpCount) {
                    fileOps = true;
                } else {
                    fileOps = false;
                }
                // use a lower fill rate if there were any file operations
                int fillRate = fileOps ? autoCompactFillRate / 3 : autoCompactFillRate;
                // TODO how to avoid endless compaction if there is a bug
                // in the bookkeeping?
                compact(fillRate, autoCommitMemory);
                autoCompactLastFileOpCount = fileStore.getWriteCount() + fileStore.getReadCount();
            } catch (Exception e) {
                if (backgroundExceptionHandler != null) {
                    backgroundExceptionHandler.uncaughtException(null, e);
                }
            }
        }
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

    public boolean isClosed() {
        return closed;
    }

    /**
     * Get the maximum memory (in bytes) used for unsaved pages. If this number
     * is exceeded, unsaved changes are stored to disk.
     * 
     * @return the memory in bytes
     */

    public int getAutoCommitMemory() {
        return autoCommitMemory;
    }

    /**
     * Get the estimated memory (in bytes) of unsaved data. If the value exceeds
     * the auto-commit memory, the changes are committed.
     * <p>
     * The returned value is an estimation only.
     * 
     * @return the memory in bytes
     */

    public int getUnsavedMemory() {
        return unsavedMemory;
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
     * Get the cache.
     * 
     * @return the cache
     */

    public CacheLongKeyLIRS<BTreePage> getCache() {
        return cache;
    }
}
