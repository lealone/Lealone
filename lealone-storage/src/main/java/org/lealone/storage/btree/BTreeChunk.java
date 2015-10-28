/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.btree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.fs.FileStorage;

/**
 * A chunk of data, containing one or multiple pages.
 * <p>
 * Chunks are page aligned (each page is usually 4096 bytes).
 * There are at most 67 million (2^26) chunks,
 * each chunk is at most 2 GB large.
 * 
 * @author H2 Group
 * @author zhh
 */
public class BTreeChunk {

    public static final int MAX_SIZE = 1 << 31 - BTreeStorage.CHUNK_HEADER_SIZE;

    private static final int FORMAT_VERSION = 1;

    /**
     * The chunk id.
     */
    public final int id;

    /**
     * The chunk creation time.
     */
    public final long creationTime;

    /**
     * The version stored in this chunk.
     */
    public long version;

    /**
     * The position of the root page.
     */
    public long rootPagePos;

    /**
     * The total number of blocks in this chunk, include chunk header(2 blocks).
     */
    public int blockCount;

    /**
     * The total number of pages in this chunk.
     */
    public int pageCount;

    /**
     * The number of pages still alive.
     */
    public int pageCountLive;

    /**
     * The sum of the max length of all pages.
     */
    public long maxLen;

    /**
     * The sum of the max length of all pages that are in use.
     */
    public long maxLenLive;

    /**
     * When this chunk was created, in milliseconds after the storage was created.
     */
    public long time;

    /**
     * When this chunk was no longer needed, in milliseconds after the storage was
     * created. After this, the chunk is kept alive a bit longer (in case it is
     * referenced in older versions).
     */
    public long unused;

    /**
     * New and old page positions.
     */
    public ArrayList<Long> pagePositions;
    public int pagePositionsOffset;
    public ArrayList<Long> leafPagePositions;
    public int leafPagePositionsOffset;
    public int leafPageCount;

    public HashSet<Long> unusedPages;

    /**
     * The garbage collection priority. Priority 0 means it needs to be
     * collected, a high value means low priority.
     */
    public int collectPriority;

    public boolean changed;

    public FileStorage fileStorage;

    BTreeChunk(int id) {
        this.id = id;
        creationTime = System.currentTimeMillis();
    }

    BTreeChunk(int id, long creationTime) {
        this.id = id;
        this.creationTime = creationTime;
    }

    /**
     * Calculate the fill rate in %. 0 means empty, 100 means full.
     *
     * @return the fill rate
     */
    public int getFillRate() {
        if (maxLenLive <= 0) {
            return 0;
        } else if (maxLenLive == maxLen) {
            return 100;
        }
        return 1 + (int) (98 * maxLenLive / maxLen);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BTreeChunk && ((BTreeChunk) o).id == id;
    }

    @Override
    public String toString() {
        return asStringBuilder().toString();
    }

    public StringBuilder asStringBuilder() {
        StringBuilder buff = new StringBuilder();

        DataUtils.appendMap(buff, "id", id);
        DataUtils.appendMap(buff, "creationTime", creationTime);
        DataUtils.appendMap(buff, "version", version);
        DataUtils.appendMap(buff, "rootPagePos", rootPagePos);

        DataUtils.appendMap(buff, "blockCount", blockCount);

        DataUtils.appendMap(buff, "pageCount", pageCount);
        if (pageCount != pageCountLive) {
            DataUtils.appendMap(buff, "pageCountLive", pageCountLive);
        }
        DataUtils.appendMap(buff, "maxLen", maxLen);
        if (maxLen != maxLenLive) {
            DataUtils.appendMap(buff, "maxLenLive", maxLenLive);
        }

        DataUtils.appendMap(buff, "time", time);
        if (unused != 0) {
            DataUtils.appendMap(buff, "unused", unused);
        }

        DataUtils.appendMap(buff, "pagePositionsOffset", pagePositionsOffset);
        DataUtils.appendMap(buff, "leafPagePositionsOffset", leafPagePositionsOffset);
        DataUtils.appendMap(buff, "leafPageCount", leafPageCount);

        DataUtils.appendMap(buff, "blockSize", BTreeStorage.BLOCK_SIZE);
        DataUtils.appendMap(buff, "format", FORMAT_VERSION);
        return buff;
    }

    /**
     * Build a chunk from the given string.
     *
     * @param s the string
     * @return the chunk
     */
    public static BTreeChunk fromString(String s) {
        HashMap<String, String> map = DataUtils.parseMap(s);
        int id = DataUtils.readHexInt(map, "id", 0);
        long creationTime = DataUtils.readHexLong(map, "creationTime", 0);
        BTreeChunk c = new BTreeChunk(id, creationTime);
        c.version = DataUtils.readHexLong(map, "version", id);
        c.rootPagePos = DataUtils.readHexLong(map, "rootPagePos", 0);

        c.blockCount = DataUtils.readHexInt(map, "blockCount", 0);

        c.pageCount = DataUtils.readHexInt(map, "pageCount", 0);
        c.pageCountLive = DataUtils.readHexInt(map, "pageCountLive", c.pageCount);

        c.maxLen = DataUtils.readHexLong(map, "maxLen", 0);
        c.maxLenLive = DataUtils.readHexLong(map, "maxLenLive", c.maxLen);

        c.time = DataUtils.readHexLong(map, "time", 0);
        c.unused = DataUtils.readHexLong(map, "unused", 0);

        c.pagePositionsOffset = DataUtils.readHexInt(map, "pagePositionsOffset", 0);
        c.leafPagePositionsOffset = DataUtils.readHexInt(map, "leafPagePositionsOffset", 0);
        c.leafPageCount = DataUtils.readHexInt(map, "leafPageCount", 0);

        long format = DataUtils.readHexLong(map, "format", FORMAT_VERSION);
        if (format > FORMAT_VERSION) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The chunk format {0} is larger " + "than the supported format {1}, "
                            + "and the file was not opened in read-only mode", format, FORMAT_VERSION);
        }

        return c;
    }
}
