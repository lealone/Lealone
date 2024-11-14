/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import com.lealone.common.compress.Compressor;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.DataUtils;
import com.lealone.db.DataBuffer;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.BTreeStorage;
import com.lealone.storage.aose.btree.chunk.Chunk;
import com.lealone.storage.aose.btree.page.PageOperations.TmpNodePage;
import com.lealone.storage.page.IPage;

public class Page implements IPage {

    /**
     * Whether assertions are enabled.
     */
    public static final boolean ASSERT = false;

    public static Page create(BTreeMap<?, ?> map, int type, ByteBuffer buff) {
        switch (type) {
        case PageUtils.PAGE_TYPE_LEAF:
            if (map.getKeyType().isKeyOnly()) {
                return new KeyPage(map);
            } else if (map.getValueType().isRowOnly()) {
                int mode = buff.get(buff.position() + 4);
                if (PageStorageMode.values()[mode] == PageStorageMode.ROW_STORAGE)
                    return new RowPage(map);
                else
                    return new ColumnsPage(map);
            } else {
                int mode = buff.get(buff.position() + 4);
                if (PageStorageMode.values()[mode] == PageStorageMode.ROW_STORAGE)
                    return new KeyValuePage(map);
                else
                    return new KeyColumnsPage(map);
            }
        case PageUtils.PAGE_TYPE_NODE:
            return new NodePage(map);
        case PageUtils.PAGE_TYPE_COLUMN:
            return new ColumnPage(map);
        default:
            throw DbException.getInternalError("type: " + type);
        }
    }

    protected final BTreeMap<?, ?> map;
    private PageReference ref;

    protected Page(BTreeMap<?, ?> map) {
        this.map = map;
    }

    public void setRef(PageReference ref) {
        this.ref = ref;
    }

    public PageReference getRef() {
        return ref;
    }

    public long getPos() {
        return ref.getPos();
    }

    private static RuntimeException ie() {
        return DbException.throwInternalError();
    }

    public Object getSplitKey(int index) {
        return getKey(index);
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

    protected Object[] getValues() {
        throw ie();
    }

    public boolean isEmpty() {
        throw ie();
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

    public boolean needSplit() {
        throw ie();
    }

    /**
     * Split the page. This modifies the current page.
     * 
     * @param at the split index
     * @return the page with the entries after the split index
     */
    public Page split(int at) {
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

    public Page copyAndInsertChild(TmpNodePage tmpNodePage) {
        throw ie();
    }

    public Page copyAndInsertLeaf(int index, Object key, Object value) {
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
     */
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        throw ie();
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     */
    public long writeUnsavedRecursive(PageInfo pInfoOld, Chunk chunk, DataBuffer buff) {
        throw ie();
    }

    protected void beforeWrite(PageInfo pInfoOld) {
        if (ASSERT) {
            if (pInfoOld.getPos() != 0)
                throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                        "Page already stored");
            if (pInfoOld.buff != null)
                DbException.throwInternalError();
        }
    }

    public void markDirty() {
        ref.markDirtyPage();
    }

    // 需要自下而上标记脏页，因为刷脏页时是自上而下的，
    // 如果标记脏页也是自上而下，有可能导致刷脏页的线程执行过快从而把最下层的脏页遗漏了。
    @Override
    public void markDirtyBottomUp() {
        markDirty();
        PageReference parentRef = getRef().getParentRef();
        while (parentRef != null) {
            parentRef.markDirtyPage();
            parentRef = parentRef.getParentRef();
        }
    }

    public int getRawChildPageCount() {
        return 0;
    }

    public int getMemory() {
        return 0;
    }

    /**
     * Create a copy of this page.
     *
     * @return a page
     */
    public Page copy() {
        throw ie();
    }

    // 只找到key对应的LeafPage就行了，不关心key是否存在
    public Page gotoLeafPage(Object key) {
        Page p = this;
        while (p.isNode()) {
            int index = p.getPageIndex(key);
            p = p.getChildPage(index);
        }
        return p;
    }

    static void readCheckValue(ByteBuffer buff, int chunkId, int offset, int pageLength) {
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest,
                    check);
        }
    }

    static void writeCheckValue(DataBuffer buff, Chunk chunk, int start, int pageLength, int checkPos) {
        int check = DataUtils.getCheckValue(chunk.id)
                ^ DataUtils.getCheckValue(chunk.getOffset() + start)
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

    long updateChunkAndPage(PageInfo pInfoOld, Chunk chunk, int start, int pageLength, int type) {
        long pos = PageUtils.getPagePos(chunk.id, chunk.getOffset() + start, type);
        chunk.pagePositionToLengthMap.put(pos, pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;
        if (chunk.sumOfPageLength > Chunk.MAX_SIZE) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", Chunk.MAX_SIZE,
                    chunk.sumOfPageLength);
        }
        ref.updatePage(pos, this, pInfoOld);
        return pos;
    }
}
