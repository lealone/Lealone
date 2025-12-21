/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.chunk;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.BitField;
import com.lealone.common.util.DataUtils;
import com.lealone.storage.aose.AOStorage;
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.aose.btree.page.PageUtils;
import com.lealone.storage.fs.FilePath;
import com.lealone.storage.fs.FileUtils;

public class ChunkManager {

    private final BTreeStorage btreeStorage;
    private final ConcurrentSkipListSet<Long> removedPages = new ConcurrentSkipListSet<>();
    private final ConcurrentHashMap<Integer, String> idToChunkFileNameMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Integer> seqToIdMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Chunk> chunks = new ConcurrentHashMap<>();
    private final BitField chunkIds = new BitField();

    private Chunk lastChunk;
    private long maxSeq;

    public ChunkManager(BTreeStorage bTreeStorage) {
        btreeStorage = bTreeStorage;
    }

    public void init(String mapBaseDir) {
        int lastChunkId = 0;
        HashMap<Integer, Long> idToSeqMap = new HashMap<>();
        File[] files = new File(mapBaseDir).listFiles();
        for (File file : files) {
            // 系统异常终止时刚创建但是还没写数据的文件
            if (file.length() == 0) {
                // file.delete(); // 这个方法第一次删除可能失败
                FileUtils.delete(file.getAbsolutePath()); // 第一次删除失败，内部会重试
                continue;
            }
            String f = file.getName();
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
                Long s = idToSeqMap.get(id);
                // 因为chunkId会复用，所以用备份的文件恢复时有可能出现多个chunkId相同但是seq不同的chunk文件
                // 此时只需要使用seq最大的那个chunk文件即可
                if (s != null) {
                    if (s < seq) {
                        idToSeqMap.put(id, seq);
                        seqToIdMap.remove(s);
                        seqToIdMap.put(seq, id);
                        idToChunkFileNameMap.put(id, f);
                    }
                } else {
                    idToSeqMap.put(id, seq);
                    seqToIdMap.put(seq, id);
                    idToChunkFileNameMap.put(id, f);
                }
            }
        }
        readLastChunk(lastChunkId);
    }

    private void readLastChunk(int lastChunkId) {
        try {
            if (lastChunkId > 0) {
                lastChunk = readChunk(lastChunkId);
            } else {
                lastChunk = null;
            }
        } catch (IllegalStateException e) {
            throw btreeStorage.panic(e);
        } catch (Exception e) {
            throw btreeStorage.panic(DataUtils.ERROR_READING_FAILED, "Failed to read last chunk: {0}",
                    lastChunkId, e);
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

    public synchronized void close() {
        for (Chunk c : chunks.values()) {
            if (c.fileStorage != null)
                c.fileStorage.close();
        }
        // maxSeq = 0;
        // for (Integer id : idToChunkFileNameMap.keySet()) {
        // chunkIds.clear(id);
        // }
        chunks.clear();
        // idToChunkFileNameMap.clear();
        removedPages.clear();
        lastChunk = null;
    }

    public synchronized void clear() {
        for (Chunk c : chunks.values()) {
            if (c.fileStorage != null) {
                c.fileStorage.close();
                c.fileStorage.delete();
            }
        }
        for (Integer id : idToChunkFileNameMap.keySet()) {
            chunkIds.clear(id);
        }
        maxSeq = 0;
        removedPages.clear();
        idToChunkFileNameMap.clear();
        seqToIdMap.clear();
        chunks.clear();
        lastChunk = null;
    }

    private synchronized Chunk readChunk(int chunkId) {
        if (chunks.containsKey(chunkId))
            return chunks.get(chunkId);
        Chunk chunk = new Chunk(chunkId);
        chunk.read(btreeStorage);
        chunks.put(chunk.id, chunk);
        return chunk;
    }

    public Chunk getChunk(long pos) {
        int chunkId = PageUtils.getPageChunkId(pos);
        return getChunk(chunkId);
    }

    public Chunk getChunk(int chunkId) {
        Chunk c = chunks.get(chunkId);
        if (c == null)
            c = readChunk(chunkId);
        if (c == null)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT, "Chunk {0} not found",
                    chunkId);
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

    public synchronized void addChunk(Chunk c) {
        chunkIds.set(c.id);
        chunks.put(c.id, c);
        idToChunkFileNameMap.put(c.id, c.fileName);
        seqToIdMap.put(getSeq(c.fileName), c.id);
    }

    public synchronized void removeUnusedChunk(Chunk c) {
        c.fileStorage.close();
        c.fileStorage.delete();
        chunkIds.clear(c.id);
        chunks.remove(c.id);
        idToChunkFileNameMap.remove(c.id);
        seqToIdMap.remove(getSeq(c.fileName));
        removedPages.removeAll(c.pagePositionToLengthMap.keySet());
        if (c == lastChunk)
            lastChunk = null;
    }

    public static Long getSeq(String chunkFileName) {
        return Long.valueOf(chunkFileName.substring(chunkFileName.lastIndexOf('_') + 1,
                chunkFileName.length() - AOStorage.SUFFIX_AO_FILE_LENGTH));
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

    public InputStream getChunkInputStream(FilePath file) {
        String name = file.getName();
        if (name.endsWith(AOStorage.SUFFIX_AO_FILE)) {
            // chunk文件名格式: c_[chunkId]_[sequence]
            String str = name.substring(2, name.length() - AOStorage.SUFFIX_AO_FILE_LENGTH);
            int pos = str.indexOf('_');
            int chunkId = Integer.parseInt(str.substring(0, pos));
            return getChunk(chunkId).fileStorage.getInputStream();
        }
        return null;
    }

    public void addRemovedPage(long pos) {
        removedPages.add(pos);
    }

    public Set<Long> getRemovedPages() {
        return removedPages;
    }

    public HashSet<Long> getAllRemovedPages() {
        HashSet<Long> removedPages = new HashSet<>(this.removedPages);
        if (lastChunk != null)
            removedPages.addAll(lastChunk.getRemovedPages());
        return removedPages;
    }

    public Set<Integer> getAllChunkIds() {
        return idToChunkFileNameMap.keySet();
    }

    public synchronized Chunk findThirdLastChunk() {
        long seq = maxSeq - 2;
        return findChunk(seq);
    }

    public synchronized Chunk findChunk(String chunkFileName) {
        Long seq = getSeq(chunkFileName);
        return findChunk(seq);
    }

    public synchronized Chunk findChunk(Long seq) {
        Integer id = seqToIdMap.get(seq);
        if (id != null)
            return getChunk(id.intValue());
        else
            return null;
    }
}
