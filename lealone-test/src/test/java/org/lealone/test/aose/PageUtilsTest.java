/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.aose;

import org.junit.Assert;
import org.junit.Test;
import org.lealone.storage.aose.btree.PageUtils;

public class PageUtilsTest extends Assert {
    @Test
    public void run() {
        int chunkId = 123;
        int offset = Integer.MAX_VALUE - 1;
        int length = 8 * 1024;
        int type = PageUtils.PAGE_TYPE_LEAF;

        long pos = PageUtils.getPagePos(chunkId, offset, length, type);

        assertEquals(chunkId, PageUtils.getPageChunkId(pos));
        assertEquals(offset, PageUtils.getPageOffset(pos));
        assertTrue(PageUtils.getPageMaxLength(pos) >= length);
        assertEquals(type, PageUtils.getPageType(pos));

        type = PageUtils.PAGE_TYPE_NODE;
        pos = PageUtils.getPagePos(chunkId, offset, length, type);
        assertEquals(type, PageUtils.getPageType(pos));

        type = PageUtils.PAGE_TYPE_REMOTE;
        pos = PageUtils.getPagePos(chunkId, offset, length, type);
        assertEquals(type, PageUtils.getPageType(pos));
    }
}
