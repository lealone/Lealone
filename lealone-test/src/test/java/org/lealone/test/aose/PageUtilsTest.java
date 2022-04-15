/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.aose;

import org.junit.Test;
import org.lealone.storage.aose.btree.page.PageUtils;

public class PageUtilsTest extends AoseTestBase {
    @Test
    public void run() {
        int chunkId = 123;
        int offset = Integer.MAX_VALUE - 1;
        int type = PageUtils.PAGE_TYPE_LEAF;

        long pos = PageUtils.getPagePos(chunkId, offset, type);

        assertEquals(chunkId, PageUtils.getPageChunkId(pos));
        assertEquals(offset, PageUtils.getPageOffset(pos));
        assertEquals(type, PageUtils.getPageType(pos));

        type = PageUtils.PAGE_TYPE_NODE;
        pos = PageUtils.getPagePos(chunkId, offset, type);
        assertEquals(type, PageUtils.getPageType(pos));

        type = PageUtils.PAGE_TYPE_REMOTE;
        pos = PageUtils.getPagePos(chunkId, offset, type);
        assertEquals(type, PageUtils.getPageType(pos));

        chunkId = 2;
        offset = 10;
        type = 1;

        pos = PageUtils.getPagePos(chunkId, offset, type);
        assertEquals(chunkId, PageUtils.getPageChunkId(pos));
        assertEquals(offset, PageUtils.getPageOffset(pos));
        assertEquals(type, PageUtils.getPageType(pos));
    }
}
