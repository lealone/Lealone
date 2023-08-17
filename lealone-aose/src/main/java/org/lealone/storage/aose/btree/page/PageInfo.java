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

    public PageInfo(Page page, long pos) {
        this.page = page;
        this.pos = pos;
    }

    public PageInfo(boolean gc, PageInfo old) {
        page = old.page;
        pos = old.pos;
        pageLength = old.pageLength;
        buff = old.buff;
        if (!gc) {
            lastTime = old.lastTime;
            hits = old.hits;
        }
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

    public long getPos() {
        return pos;
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

    public PageInfo copy(boolean gc) {
        return new PageInfo(gc, this);
    }

    public boolean isSplitted() {
        return false;
    }

    public PageReference getNewRef() {
        return null;
    }

    public static class SplittedPageInfo extends PageInfo {

        private final PageReference pRefNew;

        public SplittedPageInfo(PageReference pRefNew) {
            this.pRefNew = pRefNew;
        }

        @Override
        public boolean isSplitted() {
            return true;
        }

        @Override
        public PageReference getNewRef() {
            return pRefNew;
        }
    }
}
