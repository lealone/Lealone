/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.chunk;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.fs.FileStorage;

/**
 * A chunk of data, containing one or multiple pages.
 * <p>
 * Chunks are page aligned (each page is usually 4096 bytes).
 * There are at most 1 billion (2^30) chunks,
 * each chunk is at most 2 GB large.
 * 
 * @author H2 Group
 * @author zhh
 */
public class Chunk {
    /**
     * The block size (physical sector size) of the disk. The chunk header is
     * written twice, one copy in each block, to ensure it survives a crash.
     */
    private static final int BLOCK_SIZE = 4 * 1024;
    private static final int CHUNK_HEADER_SIZE = 2 * BLOCK_SIZE;

    public static long getFilePos(int offset) {
        long filePos = offset + CHUNK_HEADER_SIZE;
        if (filePos < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Negative position {0}", filePos);
        }
        return filePos;
    }

    public static final int MAX_SIZE = Integer.MAX_VALUE - CHUNK_HEADER_SIZE;

    private static final int FORMAT_VERSION = 2;

    /**
     * The chunk id.
     */
    public final int id;

    /**
     * The position of the root page.
     */
    public long rootPagePos;

    /**
     * The total number of pages in this chunk.
     */
    public int pageCount;

    /**
     * The sum of the length of all pages.
     */
    public long sumOfPageLength;

    public long sumOfLivePageLength;

    public int pagePositionAndLengthOffset;
    // 会有多个线程读写，不能直接用HashMap
    public final ConcurrentHashMap<Long, Integer> pagePositionToLengthMap = new ConcurrentHashMap<>();

    public FileStorage fileStorage;
    public String fileName;
    public long mapSize;
    public Long mapMaxKey; // 从FORMAT_VERSION=2时新增
    public int formatVersion = FORMAT_VERSION;

    private int removedPageOffset;
    private int removedPageCount;
    private HashSet<Long> removedPages;

    public Chunk(int id) {
        this.id = id;
    }

    public int getPageLength(long pagePosition) {
        Integer length = pagePositionToLengthMap.get(pagePosition);
        if (length == null) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, not found page {1}", fileStorage.getFileName(),
                    pagePosition);
        }
        return length.intValue();
    }

    /**
     * Calculate the fill rate in %. 
     * <p>
     * 0 means empty, 100 means full.
     *
     * @return the fill rate
     */
    int getFillRate() {
        if (sumOfLivePageLength <= 0) {
            return 0;
        } else if (sumOfLivePageLength == sumOfPageLength) {
            return 100;
        }
        return 1 + (int) (98 * sumOfLivePageLength / sumOfPageLength);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Chunk && ((Chunk) o).id == id;
    }

    @Override
    public String toString() {
        return asStringBuilder().toString();
    }

    public int getOffset() {
        int size = (int) fileStorage.size();
        if (size <= 0)
            return 0;
        else
            return size - CHUNK_HEADER_SIZE;
    }

    private void readPagePositions() {
        if (!pagePositionToLengthMap.isEmpty())
            return;
        ByteBuffer buff = fileStorage.readFully(getFilePos(pagePositionAndLengthOffset),
                pageCount * 8 + pageCount * 4);
        for (int i = 0; i < pageCount; i++) {
            long position = buff.getLong();
            int length = buff.getInt();
            pagePositionToLengthMap.put(position, length);
        }
    }

    private void writePagePositions(DataBuffer buff) {
        pagePositionAndLengthOffset = getOffset() + buff.position();
        for (Entry<Long, Integer> e : pagePositionToLengthMap.entrySet()) {
            buff.putLong(e.getKey()).putInt(e.getValue());
        }
    }

    public HashSet<Long> getRemovedPages() {
        if (removedPages == null) {
            removedPages = new HashSet<>(removedPageCount);
            if (removedPageCount > 0) {
                ByteBuffer buff = fileStorage.readFully(getFilePos(removedPageOffset),
                        removedPageCount * 8);
                for (int i = 0; i < removedPageCount; i++) {
                    removedPages.add(buff.getLong());
                }
            }
        }
        return removedPages;
    }

    private void writeRemovedPages(DataBuffer buff, ChunkManager chunkManager) {
        // 使用老的removedPageOffset读
        HashSet<Long> oldRemovedPages = getRemovedPages();
        HashSet<Long> newRemovedPages = new HashSet<>(chunkManager.getRemovedPages());
        // 更新removedPageOffset
        removedPageOffset = getOffset() + buff.position();
        removedPageCount = oldRemovedPages.size() + newRemovedPages.size();
        for (long pos : oldRemovedPages) {
            buff.putLong(pos);
        }
        for (long pos : newRemovedPages) {
            buff.putLong(pos);
        }
    }

    public void read(BTreeStorage btreeStorage) {
        if (fileStorage == null)
            fileStorage = btreeStorage.getFileStorage(id);
        readHeader();
        readPagePositions();
    }

    private void readHeader() {
        boolean ok = false;
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
                parseMap(m);
                ok = true;
                break;
            } catch (Exception e) {
                continue;
            }
        }
        if (!ok) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Chunk header is corrupt: {0}", fileStorage);
        }
    }

    private void writeHeader() {
        StringBuilder buff = asStringBuilder();
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
        fileStorage.writeFully(0, header);
    }

    private void parseMap(HashMap<String, String> map) {
        // int id = DataUtils.readHexInt(map, "id", 0);
        rootPagePos = DataUtils.readHexLong(map, "rootPagePos", 0);

        pageCount = DataUtils.readHexInt(map, "pageCount", 0);
        sumOfPageLength = DataUtils.readHexLong(map, "sumOfPageLength", 0);

        pagePositionAndLengthOffset = DataUtils.readHexInt(map, "pagePositionAndLengthOffset", 0);

        mapSize = DataUtils.readHexLong(map, "mapSize", 0);
        if (map.containsKey("mapMaxKey"))
            mapMaxKey = DataUtils.readHexLong(map, "mapMaxKey", 0);

        int format = DataUtils.readHexInt(map, "format", FORMAT_VERSION);
        if (format > FORMAT_VERSION) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The chunk format {0} is larger than the supported format {1}", format,
                    FORMAT_VERSION);
        }
        formatVersion = format;

        removedPageOffset = DataUtils.readHexInt(map, "removedPageOffset", 0);
        removedPageCount = DataUtils.readHexInt(map, "removedPageCount", 0);
    }

    private StringBuilder asStringBuilder() {
        StringBuilder buff = new StringBuilder();

        DataUtils.appendMap(buff, "id", id);
        DataUtils.appendMap(buff, "rootPagePos", rootPagePos);

        DataUtils.appendMap(buff, "pageCount", pageCount);
        DataUtils.appendMap(buff, "sumOfPageLength", sumOfPageLength);

        DataUtils.appendMap(buff, "pagePositionAndLengthOffset", pagePositionAndLengthOffset);

        DataUtils.appendMap(buff, "blockSize", BLOCK_SIZE);
        DataUtils.appendMap(buff, "mapSize", mapSize);
        DataUtils.appendMap(buff, "mapMaxKey", mapMaxKey);
        DataUtils.appendMap(buff, "format", formatVersion);

        DataUtils.appendMap(buff, "removedPageOffset", removedPageOffset);
        DataUtils.appendMap(buff, "removedPageCount", removedPageCount);

        return buff;
    }

    public synchronized void write(BTreeMap<?, ?> map, DataBuffer body, boolean appendMode,
            ChunkManager chunkManager) {
        writePagePositions(body);
        writeRemovedPages(body, chunkManager);

        // chunk header
        writeHeader();
        // chunk body
        long bodyPos = appendMode ? fileStorage.size() : CHUNK_HEADER_SIZE;
        fileStorage.writeFully(bodyPos, body.getAndFlipBuffer());
        fileStorage.sync();
    }

    // 这个方法未调用sync，上层调用者需要额外按需调用sync
    public synchronized void writeRedoLog(ByteBuffer log) {
        long size = fileStorage.size();
        long pos = size;
        if (size == 0) {
            writeHeader();
            pos = CHUNK_HEADER_SIZE;
        }
        fileStorage.writeFully(pos, log);
    }

    public synchronized ByteBuffer readRedoLog() {
        long pos = getFilePos(removedPageOffset + removedPageCount * 8);
        int len = (int) (fileStorage.size() - pos);
        if (len == 0)
            return null;
        return fileStorage.readFully(pos, len);
    }

    public synchronized void removeRedoLogAndRemovedPages(BTreeMap<?, ?> map) {
        long pos = getFilePos(removedPageOffset);
        // 只有redo log的chunk可以直接删除
        if (pos == CHUNK_HEADER_SIZE) {
            map.getBTreeStorage().getChunkManager().removeUnusedChunk(this);
            return;
        }
        long len = fileStorage.size() - pos;
        if (len != 0) {
            fileStorage.truncate(pos);
            fileStorage.sync();
        }
    }
}
