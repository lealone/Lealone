/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.util.BitField;
import org.lealone.common.util.DataUtils;
import org.lealone.storage.aose.AOStorage;

class ChunkManager {

    private final BTreeStorage btreeStorage;
    private final TreeSet<Long> removedPages = new TreeSet<>();
    private final ConcurrentHashMap<Integer, String> idToChunkFileNameMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();
    private final BitField chunkIds = new BitField();

    private Chunk lastChunk;
    private long maxSeq;

    ChunkManager(BTreeStorage bTreeStorage) {
        btreeStorage = bTreeStorage;
    }

    void init(String mapBaseDir) {
        int lastChunkId = 0;
        String[] files = new File(mapBaseDir).list();
        for (String f : files) {
            if (f.endsWith(AOStorage.SUFFIX_AO_FILE)) {
                // chunk文件名格式: c_[chunkId]_[sequence]
                String str = f.substring(2, f.length() - AOStorage.SUFFIX_AO_FILE_LENGTH);
                int pos = str.indexOf('_');
                int id = Integer.parseInt(str.substring(0, pos));
                long seq = Long.parseLong(str.substring(pos + 1));
                if (seq > maxSeq) {
                    maxSeq = seq;
                    lastChunkId = id;
                }
                chunkIds.set(id);
                idToChunkFileNameMap.put(id, f);
            }
        }
        readLastChunk(lastChunkId);
    }

    private void readLastChunk(int lastChunkId) {
        try {
            if (lastChunkId > 0) {
                lastChunk = readChunk(lastChunkId);
                lastChunk.readRemovedPages(removedPages);
            } else {
                lastChunk = null;
            }
        } catch (IllegalStateException e) {
            throw btreeStorage.panic(e);
        } catch (Exception e) {
            throw btreeStorage.panic(DataUtils.ERROR_READING_FAILED, "Failed to read last chunk: {0}", lastChunkId, e);
        }
    }

    Chunk getLastChunk() {
        return lastChunk;
    }

    void setLastChunk(Chunk lastChunk) {
        this.lastChunk = lastChunk;
    }

    String getChunkFileName(int chunkId) {
        return idToChunkFileNameMap.get(chunkId);
    }

    String createChunkFileName(int chunkId) {
        // chunk文件名格式: c_[chunkId]_[sequence]
        return "c_" + chunkId + "_" + nextSeq() + AOStorage.SUFFIX_AO_FILE;
    }

    private long nextSeq() {
        return ++maxSeq;
    }

    synchronized TreeSet<Long> getRemovedPages() {
        return removedPages;
    }

    synchronized void addRemovedPage(long pagePos) {
        removedPages.add(pagePos);
    }

    synchronized void updateRemovedPages(TreeSet<Long> removedPages) {
        this.removedPages.clear();
        this.removedPages.addAll(removedPages);
    }

    synchronized void close() {
        for (Chunk c : chunks.values()) {
            if (c.fileStorage != null)
                c.fileStorage.close();
        }
        chunks.clear();
        removedPages.clear();
        idToChunkFileNameMap.clear();
    }

    synchronized Chunk readChunk(int chunkId) {
        Chunk chunk = new Chunk(chunkId);
        chunk.readHeader(btreeStorage);
        chunk.readPagePositions();
        chunks.put(chunk.id, chunk);
        return chunk;
    }

    Chunk getChunk(long pos) {
        int chunkId = PageUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null)
            c = readChunk(chunkId);
        if (c == null)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} not found", chunkId);
        return c;
    }

    Chunk createChunk() {
        int id = chunkIds.nextClearBit(1);
        chunkIds.set(id);
        Chunk c = new Chunk(id);
        c.fileName = createChunkFileName(id);
        // chunks.put(id, c);
        return c;
    }

    void addChunk(Chunk c) {
        chunkIds.set(c.id);
        chunks.put(c.id, c);
        idToChunkFileNameMap.put(c.id, c.fileName);
    }

    Collection<Chunk> getChunks() {
        return chunks.values();
    }

    boolean containsChunk(int id) {
        return chunks.containsKey(id);
    }

    void removeUnusedChunk(Chunk c) {
        c.fileStorage.close();
        c.fileStorage.delete();
        chunkIds.clear(c.id);
        chunks.remove(c.id);
        idToChunkFileNameMap.remove(c.id);
    }

    List<Integer> getAllChunkIds() {
        return new ArrayList<>(idToChunkFileNameMap.keySet());
    }
}
