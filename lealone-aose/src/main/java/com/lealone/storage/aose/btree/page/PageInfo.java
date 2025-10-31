/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.storage.page.PageListener;

//内存占用56字节
public class PageInfo {

    public Page page;
    public long pos;

    public ByteBuffer buff;
    public int pageLength;

    public long lastTime;
    public int hits; // 只是一个预估值，不需要精确

    private volatile PageLock pageLock;

    public PageInfo() {
    }

    public PageInfo(Page page, long pos) {
        this.page = page;
        this.pos = pos;
    }

    public void updateTime() {
        lastTime = System.currentTimeMillis();
        int h = hits + 1;
        if (h < 0)
            h = 1;
        hits = h;
    }

    public PageListener getPageListener() {
        if (pageLock != null)
            return pageLock.getPageListener();
        else
            return null; // 读到了一个已经切割或删除的page
    }

    public PageLock getPageLock() {
        return pageLock;
    }

    public void setPageLock(PageLock pageLock) {
        this.pageLock = pageLock;
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
        pInfo.pageLock = pageLock;
        if (!gc) {
            pInfo.lastTime = lastTime;
            pInfo.hits = hits;
        }
        return pInfo;
    }

    public boolean isOnline() {
        return pos > 0 && (page != null || buff != null);
    }

    public boolean isDirty() {
        return pos == 0;
    }

    public boolean isSplitted() {
        return false;
    }

    // 比如发生了切割或page从父节点中删除
    public boolean isDataStructureChanged() {
        return false;
    }

    public PageReference getNewRef() {
        return null;
    }

    public static class DataStructureChangedPageInfo extends PageInfo {

        private final PageReference pRefNew;

        public DataStructureChangedPageInfo(PageReference pRefNew, PageLock pageLock) {
            this.pRefNew = pRefNew;
            setPageLock(pageLock);
        }

        @Override
        public boolean isDataStructureChanged() {
            return true;
        }

        @Override
        public PageReference getNewRef() {
            return pRefNew;
        }
    }

    public static class RemovedPageInfo extends DataStructureChangedPageInfo {

        public RemovedPageInfo(PageReference pRefNew, PageLock pageLock) {
            super(pRefNew, pageLock);
        }
    }

    public static class SplittedPageInfo extends DataStructureChangedPageInfo {

        public SplittedPageInfo(PageReference pRefNew, PageLock pageLock) {
            super(pRefNew, pageLock);
        }

        @Override
        public boolean isSplitted() {
            return true;
        }
    }
}
