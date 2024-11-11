/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

//内存占用56字节
public class PageInfo {

    public Page page;
    public long pos;

    public ByteBuffer buff;
    public int pageLength;
    public long markDirtyCount;

    public long lastTime;
    public int hits; // 只是一个预估值，不需要精确

    public PageInfo() {
    }

    public PageInfo(Page page, long pos) {
        this.page = page;
        this.pos = pos;
    }

    public void updateTime() {
        lastTime = System.currentTimeMillis();
        hits++;
        if (hits < 0)
            hits = 1;
    }

    public void updateTime(PageInfo pInfoOld) {
        lastTime = pInfoOld.lastTime;
        hits = pInfoOld.hits;
    }

    public Page getPage() {
        return page;
    }

    public long getPos() {
        return pos;
    }

    public int getPageMemory() {
        return page == null ? 0 : page.getMemory();
    }

    public int getBuffMemory() {
        return buff == null ? 0 : buff.limit();
    }

    public int getTotalMemory() {
        return getPageMemory() + getBuffMemory();
    }

    public long getLastTime() {
        return lastTime;
    }

    public int getHits() {
        return hits;
    }

    public void releaseBuff() {
        buff = null;
    }

    public void releasePage() {
        page = null;
    }

    public PageInfo copy(long newPos) {
        PageInfo pInfo = copy(false);
        pInfo.pos = newPos;
        return pInfo;
    }

    public PageInfo copy(boolean gc) {
        PageInfo pInfo = new PageInfo();
        pInfo.page = page;
        pInfo.pos = pos;
        pInfo.buff = buff;
        pInfo.pageLength = pageLength;
        pInfo.markDirtyCount = markDirtyCount;
        if (!gc) {
            pInfo.lastTime = lastTime;
            pInfo.hits = hits;
        }
        return pInfo;
    }

    public boolean isSplitted() {
        return false;
    }

    public PageReference getNewRef() {
        return null;
    }

    public PageReference getLeftRef() {
        return null;
    }

    public PageReference getRightRef() {
        return null;
    }

    public static class SplittedPageInfo extends PageInfo {

        private final PageReference pRefNew;
        private final PageReference lRef;
        private final PageReference rRef;

        public SplittedPageInfo(PageReference pRefNew, PageReference lRef, PageReference rRef) {
            this.pRefNew = pRefNew;
            this.lRef = lRef;
            this.rRef = rRef;
        }

        @Override
        public boolean isSplitted() {
            return true;
        }

        @Override
        public PageReference getNewRef() {
            return pRefNew;
        }

        @Override
        public PageReference getLeftRef() {
            return lRef;
        }

        @Override
        public PageReference getRightRef() {
            return rRef;
        }
    }
}
