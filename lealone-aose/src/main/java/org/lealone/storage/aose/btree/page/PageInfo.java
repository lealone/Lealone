/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

public class PageInfo {

    public ByteBuffer buff;
    public int pageLength;
    public long lastTime;
    public int hits; // 只是一个预估值，不需要精确

    public void updateTime() {
        lastTime = System.currentTimeMillis();
        hits++;
    }

    public int getBuffMemory() {
        return buff == null ? 0 : buff.limit();
    }
}
