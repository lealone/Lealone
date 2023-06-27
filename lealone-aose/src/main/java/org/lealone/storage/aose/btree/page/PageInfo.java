/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

public class PageInfo {

    public Page page;
    public long pos;

    public ByteBuffer buff;
    public int pageLength;
    public long lastTime;
    public int hits; // 只是一个预估值，不需要精确

    public PageInfo() {
    }

    public PageInfo(long pos) {
        this.pos = pos;
    }

    public void updateTime() {
        lastTime = System.currentTimeMillis();
        hits++;
        if (hits < 0)
            hits = 1;
    }

    public Page getPage() {
        return page;
    }

    public int getBuffMemory() {
        return buff == null ? 0 : buff.limit();
    }

    public long getLastTime() {
        return lastTime;
    }

    public int getHits() {
        return hits;
    }

    public void resetHits() {
        hits = 0;
    }

    public void releaseBuff() {
        buff = null;
    }

    public void releasePage() {
        page = null;
    }
}
