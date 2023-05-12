/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreeStorage;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.aose.btree.page.PageOperations.TmpNodePage;

public class Page {

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    public static Page create(BTreeMap<?, ?> map, long pos, ByteBuffer buff, int pageLength, int type) {
        Page p;
        if (type == PageUtils.PAGE_TYPE_LEAF)
            p = new LeafPage(map);
        else if (type == PageUtils.PAGE_TYPE_NODE)
            p = new NodePage(map);
        else if (type == PageUtils.PAGE_TYPE_COLUMN)
            p = new ColumnPage(map);
        else
            throw DbException.getInternalError("type: " + type);

        p.pos = pos;
        p.pInfo.buff = buff;
        p.pInfo.pageLength = pageLength;
        return p;
    }

    protected final PageInfo pInfo = new PageInfo();
    protected final BTreeMap<?, ?> map;
    protected long pos;

    private PageReference ref;

    protected Page(BTreeMap<?, ?> map) {
        this.map = map;
    }

    public void updateTime() {
        pInfo.updateTime();
    }

    public PageInfo getPageInfo() {
        return pInfo;
    }

    public void setRef(PageReference ref) {
        this.ref = ref;
    }

    public PageReference getRef() {
        return ref;
    }

    /**
     * Get the position of the page
     * 
     * @return the position
     */
    public long getPos() {
        return pos;
    }

    private static RuntimeException ie() {
        return DbException.throwInternalError();
    }

    /**
    * Get the key at the given index.
    * 
    * @param index the index
    * @return the key
    */
    public Object getKey(int index) {
        throw ie();
    }

    /**
     * Get the number of keys in this page.
     * 
     * @return the number of keys
     */
    public int getKeyCount() {
        throw ie();
    }

    /**
     * Get the value at the given index.
     * 
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        throw ie();
    }

    public Object getValue(int index, int[] columnIndexes) {
        throw ie();
    }

    public Object getValue(int index, boolean allColumns) {
        throw ie();
    }

    public boolean isEmpty() {
        throw ie();
    }

    public boolean isNotEmpty() {
        return !isEmpty();
    }

    /**
    * Get the total number of key-value pairs, including child pages.
    *
    * @return the number of key-value pairs
    */
    public long getTotalCount() {
        return 0;
    }

    public PageReference[] getChildren() {
        throw ie();
    }

    public PageReference getChildPageReference(int index) {
        throw ie();
    }

    /**
     * Get the child page at the given index.
     * 
     * @param index the index
     * @return the child page
     */
    public Page getChildPage(int index) {
        throw ie();
    }

    /**
     * Check whether this is a leaf page.
     * 
     * @return true if it is a leaf page
     */
    public boolean isLeaf() {
        return false;
    }

    /**
     * Check whether this is a node page.
     * 
     * @return true if it is a node page
     */
    public boolean isNode() {
        return false;
    }

    public int binarySearch(Object key) {
        throw ie();
    }

    public int getPageIndex(Object key) {
        int index = binarySearch(key);
        if (index < 0) {
            index = -index - 1;
        } else {
            index++;
        }
        return index;
    }

    boolean needSplit() {
        throw ie();
    }

    /**
     * Split the page. This modifies the current page.
     * 
     * @param at the split index
     * @return the page with the entries after the split index
     */
    Page split(int at) {
        throw ie();
    }

    /**
     * Replace the key at an index in this page.
     * 
     * @param index the index
     * @param key the new key
     */
    public void setKey(int index, Object key) {
        throw ie();
    }

    /**
     * Replace the value at an index in this page.
     * 
     * @param index the index
     * @param value the new value
     * @return the old value
     */
    public Object setValue(int index, Object value) {
        throw ie();
    }

    Page copyAndInsertChild(TmpNodePage tmpNodePage) {
        throw ie();
    }

    public Page copyLeaf(int index, Object key, Object value) {
        throw ie();
    }

    /**
     * Remove the key and value (or child) at the given index.
     * 
     * @param index the index
     */
    public void remove(int index) {
        throw ie();
    }

    /**
     * Read the page from the buffer.
     * 
     * @param buff the buffer
     * @param chunkId the chunk id
     * @param offset the offset within the chunk
     * @param expectedPageLength the expected page length
     * @param disableCheck disable check
     */
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        throw ie();
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     */
    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        throw ie();
    }

    /**
     * Unlink the children recursively after all data is written.
     */
    void writeEnd() {
    }

    public int getRawChildPageCount() {
        return 0;
    }

    public int getMemory() {
        return 0;
    }

    public int getBuffMemory() {
        return pInfo.getBuffMemory();
    }

    public int getTotalMemory() {
        return getMemory() + getBuffMemory();
    }

    /**
     * Create a copy of this page.
     *
     * @return a page
     */
    public Page copy() {
        throw ie();
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        throw ie();
    }

    public void markDirty() {
        markDirty(false);
    }

    public void markDirty(boolean hasUnsavedChanges) {
        if (pos != 0) {
            removePage();
            pos = 0;
        } else {
            // 频繁设置volatile类型的hasUnsavedChanges字段也影响性能
            if (hasUnsavedChanges)
                map.getBTreeStorage().setUnsavedChanges(true);
        }
    }

    public void markDirtyRecursive() {
        markDirty(true);
        PageReference parentRef = getRef().getParentRef();
        while (parentRef != null) {
            parentRef.page.markDirty(false);
            parentRef = parentRef.getParentRef();
        }
    }

    // 返回key所在的leaf page
    public Page binarySearchLeafPage(Object key) {
        Page p = this;
        while (true) {
            if (p.isLeaf()) {
                int index = p.binarySearch(key);
                // 如果找不到，是返回null还是throw new AssertionError()，由调用者确保key总是存在
                return index >= 0 ? p : null;
            } else {
                int index = p.getPageIndex(key);
                p = p.getChildPage(index);
            }
        }
    }

    // 只找到key对应的LeafPage就行了，不关心key是否存在
    public Page gotoLeafPage(Object key) {
        return gotoLeafPage(key, false);
    }

    public Page gotoLeafPage(Object key, boolean markDirty) {
        Page p = this;
        while (p.isNode()) {
            if (markDirty)
                p.markDirty();
            int index = p.getPageIndex(key);
            p = p.getChildPage(index);
        }
        return p;
    }

    static void readCheckValue(ByteBuffer buff, int chunkId, int offset, int pageLength,
            boolean disableCheck) {
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (!disableCheck && check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest,
                    check);
        }
    }

    static void writeCheckValue(DataBuffer buff, int chunkId, int start, int pageLength, int checkPos) {
        int check = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(start)
                ^ DataUtils.getCheckValue(pageLength);
        buff.putShort(checkPos, (short) check);
    }

    static void checkPageLength(int chunkId, int pageLength, int expectedPageLength) {
        if (pageLength != expectedPageLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId,
                    expectedPageLength, pageLength);
        }
    }

    void compressPage(DataBuffer buff, int compressStart, int type, int typePos) {
        int expLen = buff.position() - compressStart;
        if (expLen > 16) {
            BTreeStorage storage = map.getBTreeStorage();
            int compressionLevel = storage.getCompressionLevel();
            if (compressionLevel > 0) {
                Compressor compressor;
                int compressType;
                if (compressionLevel == 1) {
                    compressor = storage.getCompressorFast();
                    compressType = PageUtils.PAGE_COMPRESSED;
                } else {
                    compressor = storage.getCompressorHigh();
                    compressType = PageUtils.PAGE_COMPRESSED_HIGH;
                }
                byte[] exp = new byte[expLen];
                buff.position(compressStart).get(exp);
                byte[] comp = new byte[expLen * 2];
                int compLen = compressor.compress(exp, expLen, comp, 0);
                int plus = DataUtils.getVarIntLen(compLen - expLen);
                if (compLen + plus < expLen) {
                    buff.position(typePos).put((byte) (type + compressType));
                    buff.position(compressStart).putVarInt(expLen - compLen).put(comp, 0, compLen);
                }
            }
        }
    }

    ByteBuffer expandPage(ByteBuffer buff, int type, int start, int pageLength) {
        boolean compressed = (type & PageUtils.PAGE_COMPRESSED) != 0;
        if (compressed) {
            Compressor compressor;
            if ((type & PageUtils.PAGE_COMPRESSED_HIGH) == PageUtils.PAGE_COMPRESSED_HIGH) {
                compressor = map.getBTreeStorage().getCompressorHigh();
            } else {
                compressor = map.getBTreeStorage().getCompressorFast();
            }
            int lenAdd = DataUtils.readVarInt(buff);
            int compLen = pageLength + start - buff.position();
            byte[] comp = DataUtils.newBytes(compLen);
            buff.get(comp);
            int l = compLen + lenAdd;
            ByteBuffer newBuff = ByteBuffer.allocate(l);
            compressor.expand(comp, 0, compLen, newBuff.array(), newBuff.arrayOffset(), l);
            return newBuff;
        }
        return buff;
    }

    void updateChunkAndPage(Chunk chunk, int start, int pageLength, int type) {
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = PageUtils.getPagePos(chunk.id, start, type);
        chunk.pagePositionToLengthMap.put(pos, pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        // this will make sure nodes stays in the cache for a longer time
        updateTime();

        if (chunk.sumOfPageLength > Chunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", Chunk.MAX_SIZE,
                    chunk.sumOfPageLength);
    }

    ////////////////////// 打印出漂亮的由page组成的btree ////////////////////////////////

    public String getPrettyPageInfo(boolean readOffLinePage) {
        throw ie();
    }

    void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
    }

    static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }
}
