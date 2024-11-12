/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

public class PageUtils {

    /**
     * The type for leaf page.
     */
    public static final int PAGE_TYPE_LEAF = 0;

    /**
     * The type for node page.
     */
    public static final int PAGE_TYPE_NODE = 1;

    /**
     * The type for column page.
     */
    public static final int PAGE_TYPE_COLUMN = 2;

    /**
     * The bit mask for compressed pages (compression level fast).
     */
    public static final int PAGE_COMPRESSED = 2;

    /**
     * The bit mask for compressed pages (compression level high).
     */
    public static final int PAGE_COMPRESSED_HIGH = 2 + 4;

    /**
     * The estimated number of bytes used per child entry.
     */
    public static final int PAGE_MEMORY_CHILD = 16;

    /**
     * Get the chunk id from the position.
     *
     * @param pos the position
     * @return the chunk id
     */
    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 34);
    }

    /**
     * Get the offset from the position.
     *
     * @param pos the position
     * @return the offset
     */
    public static int getPageOffset(long pos) {
        return (int) (pos >> 2);
    }

    /**
     * Get the page type from the position.
     *
     * @param pos the position
     * @return the page type (PAGE_TYPE_NODE or PAGE_TYPE_LEAF)
     */
    public static int getPageType(long pos) {
        return ((int) pos) & 3;
    }

    /**
     * Get the position of this page. The following information is encoded in
     * the position: the chunk id, the offset, and the type (node or leaf).
     *
     * @param chunkId the chunk id
     * @param offset the offset
     * @param type the page type (1 for node, 0 for leaf)
     * @return the position
     */
    public static long getPagePos(int chunkId, int offset, int type) {
        long pos = (long) chunkId << 34; // 往右移，相当于空出来34位，chunkId占64-34=30位
        pos |= (long) offset << 2; // offset占34-2=32位
        pos |= type; // type占2位
        return pos;
    }

    public static boolean isLeafPage(long pos) {
        return pos > 0 && getPageType(pos) == PAGE_TYPE_LEAF;
    }

    public static boolean isNodePage(long pos) {
        return pos > 0 && getPageType(pos) == PAGE_TYPE_NODE;
    }
}
