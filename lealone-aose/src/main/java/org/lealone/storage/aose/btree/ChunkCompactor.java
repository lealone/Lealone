/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

/**
 * Try to increase the fill rate by re-writing partially full chunks. 
 * Chunks with a low number of live items are re-written.
 * <p>
 * If the current fill rate is higher than the minimum fill rate, nothing is done.
 */
class ChunkCompactor {

    private final BTreeStorage btreeStorage;
    private final ChunkManager chunkManager;

    ChunkCompactor(BTreeStorage btreeStorage, ChunkManager chunkManager) {
        this.btreeStorage = btreeStorage;
        this.chunkManager = chunkManager;
    }

    void executeCompact() {
        TreeSet<Long> removedPages = chunkManager.getRemovedPagesCopy();
        if (removedPages.isEmpty())
            return;

        // 读取被删除了至少一个page的chunk的元数据
        List<Chunk> chunks = readChunks(removedPages);

        // 如果chunk中的page都被标记为删除了，说明这个chunk已经不再使用了，可以直接删除它
        List<Chunk> unusedChunks = findUnusedChunks(chunks, removedPages);
        if (!unusedChunks.isEmpty()) {
            removeUnusedChunks(unusedChunks, removedPages);
            chunks.removeAll(unusedChunks);
        }

        // 看看哪些chunk中未被删除的page占比<=MinFillRate，然后重写它们到一个新的chunk中
        rewrite(chunks, removedPages);
    }

    private List<Chunk> readChunks(TreeSet<Long> removedPages) {
        HashSet<Integer> chunkIds = new HashSet<>();
        for (Long pagePos : removedPages) {
            if (!PageUtils.isNodePage(pagePos))
                chunkIds.add(PageUtils.getPageChunkId(pagePos));
        }
        return chunkManager.readChunks(chunkIds);
    }

    // 在这里顺便把LivePage的总长度都算好了
    private List<Chunk> findUnusedChunks(List<Chunk> chunks, TreeSet<Long> removedPages) {
        ArrayList<Chunk> unusedChunks = new ArrayList<>();
        for (Chunk c : chunks) {
            c.sumOfLivePageLength = 0;
            boolean unused = true;
            for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                if (!removedPages.contains(e.getKey())) {
                    c.sumOfLivePageLength += e.getValue();
                    unused = false;
                }
            }
            if (unused)
                unusedChunks.add(c);
        }
        return unusedChunks;
    }

    private void removeUnusedChunks(List<Chunk> unusedChunks, TreeSet<Long> removedPages) {
        if (removedPages.isEmpty())
            return;
        int size = removedPages.size();
        for (Chunk c : unusedChunks) {
            chunkManager.removeUnusedChunk(c);
            removedPages.removeAll(c.pagePositionToLengthMap.keySet());
        }
        if (size > removedPages.size()) {
            chunkManager.updateRemovedPages(removedPages);
        }
    }

    private void rewrite(List<Chunk> chunks, TreeSet<Long> removedPages) {
        // minFillRate <= 0时相当于禁用rewrite了，removedPages为空说明没有page被删除了
        if (btreeStorage.getMinFillRate() <= 0 || removedPages.isEmpty())
            return;

        List<Chunk> old = getRewritableChunks(chunks);
        boolean saveIfNeeded = false;
        for (Chunk c : old) {
            for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                long pos = e.getKey();
                if (PageUtils.isLeafPage(pos) && !removedPages.contains(pos)) {
                    BTreePage p = btreeStorage.readPage(pos);
                    p.markDirtyRecursive(); // 直接标记为脏页即可，不用更新元素
                    saveIfNeeded = true;
                }
            }
        }
        if (saveIfNeeded) {
            btreeStorage.executeSave(false);
            removedPages = chunkManager.getRemovedPagesCopy();
            removeUnusedChunks(old, removedPages);
        }
    }

    // 按chunk的FillRate从小到大排序，然后选一批chunk出来重写，并且这批chunk重写后的总长度不能超过一个chunk的容量
    private List<Chunk> getRewritableChunks(List<Chunk> chunks) {
        int minFillRate = btreeStorage.getMinFillRate();
        List<Chunk> old = new ArrayList<>();
        for (Chunk c : chunks) {
            if (c.getFillRate() > minFillRate)
                continue;
            old.add(c);
        }
        if (old.isEmpty())
            return old;

        Collections.sort(old, (o1, o2) -> {
            long comp = o1.getFillRate() - o2.getFillRate();
            if (comp == 0) {
                comp = o1.sumOfLivePageLength - o2.sumOfLivePageLength;
            }
            return Long.signum(comp);
        });

        long bytes = 0;
        int index = 0;
        int size = old.size();
        long maxBytesToWrite = Chunk.MAX_SIZE;
        for (; index < size; index++) {
            bytes += old.get(index).sumOfLivePageLength;
            if (bytes > maxBytesToWrite) // 不能超过chunk的最大容量
                break;
        }
        return index == size ? old : old.subList(0, index + 1);
    }
}
