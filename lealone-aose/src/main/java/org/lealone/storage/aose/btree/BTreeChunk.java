/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.storage.aose.btree;

import java.util.HashMap;

import org.lealone.common.util.DataUtils;
import org.lealone.storage.fs.FileStorage;

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
public class BTreeChunk {

    public static final int MAX_SIZE = Integer.MAX_VALUE - BTreeStorage.CHUNK_HEADER_SIZE;

    private static final int FORMAT_VERSION = 1;

    /**
     * The chunk id.
     */
    public final int id;

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
     * The sum of the length of all pages.
     */
    public long sumOfPageLength;

    public long sumOfLivePageLength;

    public int pagePositionAndLengthOffset;
    public final HashMap<Long, Integer> pagePositionToLengthMap = new HashMap<>();

    public FileStorage fileStorage;
    public long mapSize;

    BTreeChunk(int id) {
        this.id = id;
    }

    int getPageLength(long pagePosition) {
        return pagePositionToLengthMap.get(pagePosition);
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
        return o instanceof BTreeChunk && ((BTreeChunk) o).id == id;
    }

    @Override
    public String toString() {
        return asStringBuilder().toString();
    }

    public StringBuilder asStringBuilder() {
        StringBuilder buff = new StringBuilder();

        DataUtils.appendMap(buff, "id", id);
        DataUtils.appendMap(buff, "rootPagePos", rootPagePos);

        DataUtils.appendMap(buff, "blockCount", blockCount);

        DataUtils.appendMap(buff, "pageCount", pageCount);
        DataUtils.appendMap(buff, "sumOfPageLength", sumOfPageLength);

        DataUtils.appendMap(buff, "pagePositionAndLengthOffset", pagePositionAndLengthOffset);

        DataUtils.appendMap(buff, "blockSize", BTreeStorage.BLOCK_SIZE);
        DataUtils.appendMap(buff, "mapSize", mapSize);
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
        BTreeChunk c = new BTreeChunk(id);
        c.rootPagePos = DataUtils.readHexLong(map, "rootPagePos", 0);

        c.blockCount = DataUtils.readHexInt(map, "blockCount", 0);

        c.pageCount = DataUtils.readHexInt(map, "pageCount", 0);
        c.sumOfPageLength = DataUtils.readHexLong(map, "sumOfPageLength", 0);

        c.pagePositionAndLengthOffset = DataUtils.readHexInt(map, "pagePositionAndLengthOffset", 0);

        c.mapSize = DataUtils.readHexLong(map, "mapSize", 0);

        long format = DataUtils.readHexLong(map, "format", FORMAT_VERSION);
        if (format > FORMAT_VERSION) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_UNSUPPORTED_FORMAT,
                    "The chunk format {0} is larger than the supported format {1}", format, FORMAT_VERSION);
        }
        return c;
    }
}
