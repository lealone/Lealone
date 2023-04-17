/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.cache;

import org.lealone.storage.aose.btree.page.Page;
import org.lealone.storage.aose.btree.page.PageUtils;

public class CacheManager {

    /**
     * The page cache. The default size is 16 MB, and the average size is 2 KB.
     * It is split in 16 segments. The stack move distance is 2% of the expected number of entries.
     */
    private final Cache<Page> hotCache;
    private final Cache<Page> warmCache;

    public CacheManager(int cacheSize) {
        Cache.Config cc = new Cache.Config();
        cc.maxMemory = cacheSize / 10 * 6;
        hotCache = new Cache<>(cc);
        hotCache.setCacheListener(p -> onHotEvict(p));
        cc = new Cache.Config();
        cc.maxMemory = cacheSize / 10 * 4;
        warmCache = new Cache<>(cc);
        warmCache.setCacheListener(p -> onWarmEvict(p));
    }

    private void onHotEvict(Page p) {
        if (p != null && p.isLeaf() && p.getRef() != null) {
            p.clear();
            p.getRef().replacePage(null);
            warmCache.put(p.getPos(), p, p.getBuffMemory());
        }
    }

    private void onWarmEvict(Page p) {
        if (p != null && p.isLeaf() && p.getRef() != null) {
            p.getRef().clearBuff();
        }
    }

    public void removeCache(long pos, int memory) {
        if (PageUtils.isLeafPage(pos)) {
            // keep nodes in the cache, because they are still used for garbage collection
            hotCache.remove(pos);
            warmCache.remove(pos);
        }
    }

    public void removeWarmCache(long pos) {
        warmCache.remove(pos);
    }

    public void cachePage(long pos, Page page) {
        hotCache.put(pos, page, page.getTotalMemory());
    }

    public Page getCachePage(long pos) {
        Page p = hotCache.get(pos);
        if (p == null)
            p = warmCache.get(pos);
        return p;
    }

    public int getCacheSize() {
        return getCacheSize(hotCache) + getCacheSize(warmCache);
    }

    private int getCacheSize(Cache<?> c) {
        return (int) (c.getMaxMemory() / 1024 / 1024);
    }

    public void setCacheSize(int mb) {
        hotCache.setMaxMemory(mb * 1024 * 1024L / 10 * 6);
        hotCache.clear();
        warmCache.setMaxMemory(mb * 1024 * 1024L / 10 * 4);
        warmCache.clear();
    }

    public long getMemorySpaceUsed() {
        return hotCache.getUsedMemory() + warmCache.getUsedMemory();
    }

    public void closeCache() {
        // release memory early - this is important when called
        // because of out of memory
        hotCache.clear();
        warmCache.clear();
    }
}
