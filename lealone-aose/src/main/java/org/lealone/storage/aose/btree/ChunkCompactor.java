/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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

        removeUnusedChunks(removedPages);

        if (btreeStorage.getMinFillRate() <= 0 || removedPages.isEmpty())
            return;

        List<Chunk> old = getOldChunks();
        if (!old.isEmpty()) {
            boolean saveIfNeeded = rewrite(old, removedPages);
            if (saveIfNeeded) {
                btreeStorage.executeSave(false);
                removedPages = chunkManager.getRemovedPagesCopy();
                removeUnusedChunks(removedPages);
            }
        }
    }

    private void removeUnusedChunks(TreeSet<Long> removedPages) {
        int size = removedPages.size();
        for (Chunk c : findUnusedChunks(removedPages)) {
            chunkManager.removeUnusedChunk(c);
            removedPages.removeAll(c.pagePositionToLengthMap.keySet());
        }

        if (size > removedPages.size()) {
            chunkManager.updateRemovedPages(removedPages);
        }
    }

    private ArrayList<Chunk> findUnusedChunks(TreeSet<Long> removedPages) {
        ArrayList<Chunk> unusedChunks = new ArrayList<>();
        if (removedPages.isEmpty())
            return unusedChunks;

        readAllChunks();

        for (Chunk c : chunkManager.getChunks()) {
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

    private void readAllChunks() {
        for (int id : chunkManager.getAllChunkIds()) {
            if (!chunkManager.containsChunk(id)) {
                chunkManager.readChunk(id);
            }
        }
    }

    private List<Chunk> getOldChunks() {
        long maxBytesToWrite = Chunk.MAX_SIZE;
        List<Chunk> old = new ArrayList<>();
        for (Chunk c : chunkManager.getChunks()) {
            if (c.getFillRate() > btreeStorage.getMinFillRate())
                continue;
            old.add(c);
        }
        if (old.isEmpty())
            return old;

        Collections.sort(old, new Comparator<Chunk>() {
            @Override
            public int compare(Chunk o1, Chunk o2) {
                long comp = o1.getFillRate() - o2.getFillRate();
                if (comp == 0) {
                    comp = o1.sumOfLivePageLength - o2.sumOfLivePageLength;
                }
                return Long.signum(comp);
            }
        });

        long bytes = 0;
        int index = 0;
        int size = old.size();
        for (; index < size; index++) {
            bytes += old.get(index).sumOfLivePageLength;
            if (bytes > maxBytesToWrite)
                break;
        }
        return index == size ? old : old.subList(0, index + 1);
    }

    private boolean rewrite(List<Chunk> old, TreeSet<Long> removedPages) {
        boolean saveIfNeeded = false;
        BTreeMap<Object, Object> map = btreeStorage.getMap();
        for (Chunk c : old) {
            for (Entry<Long, Integer> e : c.pagePositionToLengthMap.entrySet()) {
                long pos = e.getKey();
                if (PageUtils.isLeafPage(pos)) {
                    if (!removedPages.contains(pos)) {
                        BTreePage p = btreeStorage.readPage(pos);
                        if (p.getKeyCount() > 0) {
                            Object key = p.getKey(0);
                            Object value = map.get(key);
                            if (value != null && map.replace(key, value, value))
                                saveIfNeeded = true;
                        }
                    }
                }
            }
        }
        return saveIfNeeded;
    }
}
