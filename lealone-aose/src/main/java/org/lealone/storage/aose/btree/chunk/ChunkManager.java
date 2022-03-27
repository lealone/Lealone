/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.chunk;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.BitField;
import org.lealone.common.util.DataUtils;
import org.lealone.storage.aose.AOStorage;
import org.lealone.storage.aose.btree.BTreeStorage;
import org.lealone.storage.aose.btree.page.PageUtils;

public class ChunkManager {

    private final BTreeStorage btreeStorage;
    private final TreeSet<Long> removedPages = new TreeSet<>();
    private final ConcurrentHashMap<Integer, String> idToChunkFileNameMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();
    private final BitField chunkIds = new BitField();

    private Chunk lastChunk;
    private long maxSeq;

    public ChunkManager(BTreeStorage bTreeStorage) {
        btreeStorage = bTreeStorage;
    }

    public void init(String mapBaseDir) {
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

    public Chunk getLastChunk() {
        return lastChunk;
    }

    public void setLastChunk(Chunk lastChunk) {
        this.lastChunk = lastChunk;
    }

    public String getChunkFileName(int chunkId) {
        String f = idToChunkFileNameMap.get(chunkId);
        if (f == null)
            throw DbException.getInternalError();
        return f;
    }

    String createChunkFileName(int chunkId) {
        // chunk文件名格式: c_[chunkId]_[sequence]
        return "c_" + chunkId + "_" + nextSeq() + AOStorage.SUFFIX_AO_FILE;
    }

    private long nextSeq() {
        return ++maxSeq;
    }

    synchronized TreeSet<Long> getRemovedPagesCopy() {
        return new TreeSet<>(removedPages);
    }

    public synchronized TreeSet<Long> getRemovedPages() {
        return removedPages;
    }

    public synchronized void addRemovedPage(long pagePos) {
        removedPages.add(pagePos);
    }

    synchronized void updateRemovedPages(TreeSet<Long> removedPages) {
        this.removedPages.clear();
        this.removedPages.addAll(removedPages);
        getLastChunk().updateRemovedPages(removedPages);
    }

    public synchronized void close() {
        for (Chunk c : chunks.values()) {
            if (c.fileStorage != null)
                c.fileStorage.close();
        }
        chunks.clear();
        removedPages.clear();
        idToChunkFileNameMap.clear();
    }

    private synchronized Chunk readChunk(int chunkId) {
        Chunk chunk = new Chunk(chunkId);
        chunk.read(btreeStorage);
        chunks.put(chunk.id, chunk);
        return chunk;
    }

    public Chunk getChunk(long pos) {
        int chunkId = PageUtils.getPageChunkId(pos);
        Chunk c = chunks.get(chunkId);
        if (c == null)
            c = readChunk(chunkId);
        if (c == null)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} not found", chunkId);
        return c;
    }

    public Chunk createChunk() {
        int id = chunkIds.nextClearBit(1);
        chunkIds.set(id);
        Chunk c = new Chunk(id);
        c.fileName = createChunkFileName(id);
        // chunks.put(id, c);
        return c;
    }

    public void addChunk(Chunk c) {
        chunkIds.set(c.id);
        chunks.put(c.id, c);
        idToChunkFileNameMap.put(c.id, c.fileName);
    }

    void removeUnusedChunk(Chunk c) {
        c.fileStorage.close();
        c.fileStorage.delete();
        chunkIds.clear(c.id);
        chunks.remove(c.id);
        idToChunkFileNameMap.remove(c.id);
    }

    List<Chunk> readChunks(HashSet<Integer> chunkIds) {
        ArrayList<Chunk> list = new ArrayList<>(chunkIds.size());
        for (int id : chunkIds) {
            if (!chunks.containsKey(id)) {
                readChunk(id);
            }
            list.add(chunks.get(id));
        }
        return list;
    }
}
