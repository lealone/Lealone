/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (http://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.lealone.storage.aose.btree;

/**
 * 
 * @author H2 Group
 * @author zhh
 */
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
     * The type for remote page.
     */
    public static final int PAGE_TYPE_REMOTE = 2;

    /**
     * The bit mask for compressed pages (compression level fast).
     */
    public static final int PAGE_COMPRESSED = 2;

    /**
     * The bit mask for compressed pages (compression level high).
     */
    public static final int PAGE_COMPRESSED_HIGH = 2 + 4;

    /**
     * The estimated number of bytes used per page object.
     */
    public static final int PAGE_MEMORY = 128;

    /**
     * The estimated number of bytes used per child entry.
     */
    public static final int PAGE_MEMORY_CHILD = 16;

    /**
     * The marker size of a very large page.
     */
    public static final int PAGE_LARGE = 2 * 1024 * 1024;

    /**
     * Get the chunk id from the position.
     *
     * @param pos the position
     * @return the chunk id
     */
    public static int getPageChunkId(long pos) {
        return (int) (pos >>> 39);
    }

    /**
     * Get the offset from the position.
     *
     * @param pos the position
     * @return the offset
     */
    public static int getPageOffset(long pos) {
        return (int) (pos >> 7);
    }

    /**
     * Get the maximum length for the given code.
     * For the code 31, PAGE_LARGE is returned.
     *
     * @param pos the position
     * @return the maximum length
     */
    public static int getPageMaxLength(long pos) {
        int code = (int) ((pos >> 2) & 31);
        if (code == 31) {
            return PAGE_LARGE;
        }
        return (2 + (code & 1)) << ((code >> 1) + 4);
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
     * the position: the chunk id, the offset, the maximum length, and the type
     * (node or leaf).
     *
     * @param chunkId the chunk id
     * @param offset the offset
     * @param length the length
     * @param type the page type (1 for node, 0 for leaf)
     * @return the position
     */
    public static long getPagePos(int chunkId, int offset, int length, int type) {
        long pos = (long) chunkId << 39; // 往右移，相当于空出来38位，chunkId占64-39=25位
        pos |= (long) offset << 7; // offset占39-7=32位
        pos |= encodeLength(length) << 2; // encodeLength(length)占7-2=5位
        pos |= type; // type占2位
        return pos;
    }

    /**
     * Convert the length to a length code 0..31. 31 means more than 1 MB.
     *
     * @param len the length
     * @return the length code
     */
    private static int encodeLength(int len) {
        if (len <= 32) {
            return 0;
        }
        int code = Integer.numberOfLeadingZeros(len);
        int remaining = len << (code + 1);
        code += code;
        if ((remaining & (1 << 31)) != 0) {
            code--;
        }
        if ((remaining << 1) != 0) {
            code--;
        }
        code = Math.min(31, 52 - code);
        // alternative code (slower):
        // int x = len;
        // int shift = 0;
        // while (x > 3) {
        // shift++;
        // x = (x >>> 1) + (x & 1);
        // }
        // shift = Math.max(0, shift - 4);
        // int code = (shift << 1) + (x & 1);
        // code = Math.min(31, code);
        return code;
    }
}
