/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.RunMode;
import org.lealone.db.value.ValueString;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreeStorage;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.aose.btree.page.PageOperations.TmpNodePage;
import org.lealone.storage.fs.FileStorage;
import org.lealone.storage.page.LeafPageMovePlan;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;

//所有的子类都不是多线程安全的，但是设计层面会保证对每个Page的更新都只由一个线程负责，
//每个Page对应一个PageOperationHandler，由它处理对Page产生的操作。
public class Page {

    public static class DynamicInfo {
        public final State state;
        public final Page redirect;

        public DynamicInfo() {
            this(State.NORMAL, null);
        }

        public DynamicInfo(State state) {
            this(state, null);
        }

        public DynamicInfo(State state, Page redirect) {
            this.state = state;
            this.redirect = redirect;
        }

        @Override
        public String toString() {
            return "DynamicInfo[" + state + "]";
        }

        boolean isSplitted() {
            return state == State.SPLITTED;
        }

        boolean isRemoving() {
            return state == State.REMOVING;
        }

        boolean isRemoved() {
            return state == State.REMOVED;
        }
    }

    public static enum State {
        NORMAL,
        SPLITTING,
        SPLITTED,
        REMOVING,
        REMOVED;

        Page redirect;

        public Page getRedirect() {
            return redirect;
        }

        public void setRedirect(Page redirect) {
            this.redirect = redirect;
        }
    }

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    private static final AtomicReferenceFieldUpdater<Page, DynamicInfo> dynamicInfoUpdater = AtomicReferenceFieldUpdater
            .newUpdater(Page.class, DynamicInfo.class, "dynamicInfo");
    protected final BTreeMap<?, ?> map;
    protected final PageOperationHandler handler;
    protected long pos;

    volatile DynamicInfo dynamicInfo = new DynamicInfo();

    AtomicReference<PageReference> parentRefRef = new AtomicReference<>();
    private PageReference ref;

    protected Page(BTreeMap<?, ?> map) {
        this(map, null);
    }

    protected Page(BTreeMap<?, ?> map, PageOperationHandler handler) {
        this.map = map;
        if (handler != null) {
            this.handler = handler;
        } else {
            if (isLeaf())
                this.handler = map.getPohFactory().getPageOperationHandler();
            else if (isNode())
                this.handler = map.getNodePageOperationHandler();
            else
                this.handler = null;
        }
    }

    public DynamicInfo getDynamicInfo() {
        return dynamicInfo;
    }

    void setParentRef(PageReference parentRef) {
        parentRefRef.set(parentRef);
    }

    public PageReference getParentRef() {
        return parentRefRef.get();
    }

    void setRef(PageReference ref) {
        this.ref = ref;
    }

    PageReference getRef() {
        return ref;
    }

    public PageOperationHandler getHandler() {
        return handler;
    }

    void addPageOperation(PageOperation po) {
        if (handler != null) {
            handler.handlePageOperation(po);
        }
    }

    // 这种情况发生在leaf page的处理器队列中有过期的由其他处理器移交过来的Put操作，
    // 但是当前处理器已经把原来的leaf page切割了，此时需要从一个重定向后的临时node page找到最新的leaf page，
    // 有可能发生多次切割，所以需要用循环来遍历
    public Page redirectIfSplited(Object key) {
        Page p = this;
        while (p.dynamicInfo.state == Page.State.SPLITTED) {
            p = p.dynamicInfo.redirect;
            int index;
            if (map.getKeyType().compare(key, p.getKey(0)) < 0)
                index = 0;
            else
                index = 1;
            p = p.getChildPage(index);
        }
        return p;
    }

    Page redirectIfSplited(boolean first) {
        Page p = this;
        while (p.dynamicInfo.state == Page.State.SPLITTED) {
            p = p.dynamicInfo.redirect.getChildPage(first ? 0 : 1);
        }
        return p;
    }

    public Page tmpCopyIfSplited() {
        Page p = this;
        if (p.dynamicInfo.state == Page.State.SPLITTED) {
            p = p.dynamicInfo.redirect;
            LeafPage left = (LeafPage) p.getChildPage(0);
            LeafPage right = (LeafPage) p.getChildPage(1);
            int length = left.keys.length + right.keys.length;
            Object[] keys = new Object[length];
            Object[] values = new Object[length];
            System.arraycopy(left.keys, 0, keys, 0, left.keys.length);
            System.arraycopy(right.keys, 0, keys, left.keys.length, right.keys.length);
            System.arraycopy(left.getValues(), 0, values, 0, left.getValues().length);
            System.arraycopy(right.getValues(), 0, values, left.getValues().length, right.getValues().length);
            p = LeafPage.create(p.map, keys, values, length, left.getMemory() + right.getMemory());
        }
        return p;
    }

    boolean updateDynamicInfo(DynamicInfo expect, DynamicInfo update) {
        return dynamicInfoUpdater.compareAndSet(this, expect, update);
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

    public Object[] getKeys() {
        throw ie();
    }

    public Object[] getValues() {
        throw ie();
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
     * Get the value at the given index.
     * 
     * @param index the index
     * @return the value
     */
    public Object getValue(int index) {
        throw ie();
    }

    public Object getValue(int index, int columnIndex) {
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
    @Deprecated
    public long getTotalCount() {
        return 0;
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
     * Get the child page at the given index.
     * 
     * @param index the index
     * @return the child page
     */
    public Page getChildPage(int index) {
        throw ie();
    }

    public PageReference getChildPageReference(int index) {
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

    /**
     * Check whether this is a remote page.
     * 
     * @return true if it is a remote page
     */
    public boolean isRemote() {
        return false;
    }

    /**
     * Search the key in this page using a binary search. Instead of always
     * starting the search in the middle, the last found index is cached.
     * <p>
     * If the key was found, the returned value is the index in the key array.
     * If not found, the returned value is negative, where -1 means the provided
     * key is smaller than any keys in this page. See also Arrays.binarySearch.
     * 
     * @param key the key
     * @return the value or null
     */
    public int binarySearch(Object key) {
        throw ie();
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

    /**
     * Replace the child page.
     * 
     * @param index the index
     * @param c the new child page
     */
    public void setChild(int index, Page c) {
        throw ie();
    }

    public void setChild(int index, PageReference ref) {
        throw ie();
    }

    void setAndInsertChild(int index, TmpNodePage tmpNodePage) {
        throw ie();
    }

    /**
     * Insert a key-value pair into this leaf.
     * 
     * @param index the index
     * @param key the key
     * @param value the value
     */
    public void insertLeaf(int index, Object key, Object value) {
        throw ie();
    }

    public Page copyLeaf(int index, Object key, Object value) {
        throw ie();
    }

    /**
     * Insert a child page into this node.
     * 
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, Page childPage) {
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
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength, boolean disableCheck) {
        throw ie();
    }

    public void writeLeaf(DataBuffer buff, boolean remote) {
        throw ie();
    }

    public static Page readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        int type = page.get();
        if (type == PageUtils.PAGE_TYPE_LEAF)
            return LeafPage.readLeafPage(map, page);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            return RemotePage.readLeafPage(map, page);
        else
            throw DbException.getInternalError("type: " + type);
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

    void markDirty() {
        if (pos != 0) {
            removePage();
            pos = 0;
        } else {
            map.getBtreeStorage().setUnsavedChanges(true);
        }
    }

    public void markDirtyRecursive() {
        markDirty();
        PageReference parentRef = getParentRef();
        while (parentRef != null) {
            parentRef.page.markDirty();
            parentRef = parentRef.page.getParentRef();
        }
        map.getRootPage().markDirty();
    }

    /**
     * Remove this page and all child pages.
     */
    public void removeAllRecursive() {
        throw ie();
    }

    public String getPrettyPageInfo(boolean readOffLinePage) {
        throw ie();
    }

    /**
     * Read a page.
     * 
     * @param map the map
     * @param fileStorage the file storage
     * @param pos the position
     * @param filePos the position in the file
     * @param pageLength the page length
     * @return the page
     */
    public static Page read(BTreeMap<?, ?> map, FileStorage fileStorage, long pos, long filePos, int pageLength) {
        ByteBuffer buff = readPageBuff(fileStorage, filePos, pageLength);
        int type = PageUtils.getPageType(pos);
        Page p = create(map, type);
        p.pos = pos;
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, pageLength, false);
        return p;
    }

    private static ByteBuffer readPageBuff(FileStorage fileStorage, long filePos, int pageLength) {
        if (pageLength < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1} ", pageLength, filePos);
        }
        return fileStorage.readFully(filePos, pageLength);
    }

    private static Page create(BTreeMap<?, ?> map, int type) {
        Page p;
        if (type == PageUtils.PAGE_TYPE_LEAF)
            p = new LeafPage(map);
        else if (type == PageUtils.PAGE_TYPE_NODE)
            p = new NodePage(map);
        else if (type == PageUtils.PAGE_TYPE_COLUMN)
            p = new ColumnPage(map);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            p = new RemotePage(map);
        else
            throw DbException.getInternalError("type: " + type);
        return p;
    }

    // 返回key所在的leaf page
    public Page binarySearchLeafPage(Object key) {
        Page p = this;
        while (true) {
            int index = p.binarySearch(key);
            if (p.isLeaf()) {
                // 如果找不到，是返回null还是throw new AssertionError()，由调用者确保key总是存在
                return index >= 0 ? p : null;
            } else {
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                p = p.getChildPage(index);
            }
        }
    }

    // 只找到key对应的LeafPage就行了，不关心key是否存在
    public Page gotoLeafPage(Object key) {
        Page p = this;
        while (p.isNode()) {
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            p = p.getChildPage(index);
        }
        return p;
    }

    public void readRemotePages() {
        throw ie();
    }

    public void moveAllLocalLeafPages(String[] oldNodes, String[] newNodes, RunMode newRunMode) {
        throw ie();
    }

    public void replicatePage(DataBuffer buff) {
        throw ie();
    }

    public static Page readReplicatedPage(BTreeMap<?, ?> map, ByteBuffer buff) {
        int type = buff.get();
        Page p = create(map, type);
        int chunkId = 0;
        int offset = buff.position();
        int pageLength = buff.getInt();
        p.read(buff, chunkId, offset, pageLength, true);
        return p;
    }

    public boolean isRemoteChildPage(int index) {
        return false;
    }

    public boolean isNodeChildPage(int index) {
        return false;
    }

    public boolean isLeafChildPage(int index) {
        return false;
    }

    public Object getLastKey() {
        throw ie();
    }

    public PageReference[] getChildren() {
        throw ie();
    }

    public void setReplicationHostIds(List<String> replicationHostIds) {
    }

    public List<String> getReplicationHostIds() {
        return null;
    }

    static void writeReplicationHostIds(List<String> replicationHostIds, DataBuffer buff) {
        if (replicationHostIds == null || replicationHostIds.isEmpty())
            buff.putInt(0);
        else {
            buff.putInt(replicationHostIds.size());
            for (String id : replicationHostIds) {
                ValueString.type.write(buff, id);
            }
        }
    }

    static List<String> readReplicationHostIds(ByteBuffer buff) {
        int length = buff.getInt();
        List<String> replicationHostIds = new ArrayList<>(length);
        for (int i = 0; i < length; i++)
            replicationHostIds.add(ValueString.type.read(buff));

        if (replicationHostIds.isEmpty())
            replicationHostIds = null;

        return replicationHostIds;
    }

    public static LeafPage createLeaf(BTreeMap<?, ?> map, Object[] keys, Object[] values, long totalCount, int memory) {
        return LeafPage.create(map, keys, values, totalCount, memory);
    }

    public static NodePage createNode(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        return NodePage.create(map, keys, children, memory);
    }

    static class PrettyPageInfo {
        StringBuilder buff = new StringBuilder();
        int pageCount;
        int leafPageCount;
        int nodePageCount;
        int levelCount;
        boolean readOffLinePage;
    }

    void getPrettyPageInfoRecursive(String indent, PrettyPageInfo info) {
    }

    public LeafPageMovePlan getLeafPageMovePlan() {
        return null;
    }

    public void setLeafPageMovePlan(LeafPageMovePlan leafPageMovePlan) {
    }

    static void readCheckValue(ByteBuffer buff, int chunkId, int offset, int pageLength, boolean disableCheck) {
        short check = buff.getShort();
        int checkTest = DataUtils.getCheckValue(chunkId) ^ DataUtils.getCheckValue(offset)
                ^ DataUtils.getCheckValue(pageLength);
        if (!disableCheck && check != (short) checkTest) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected check value {1}, got {2}", chunkId, checkTest, check);
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
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, expectedPageLength,
                    pageLength);
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

    void updateChunkAndCachePage(Chunk chunk, int start, int pageLength, int type) {
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = PageUtils.getPagePos(chunk.id, start, type);
        chunk.pagePositionToLengthMap.put(pos, pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        map.getBTreeStorage().cachePage(pos, this, getMemory());

        if (chunk.sumOfPageLength > Chunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", Chunk.MAX_SIZE, chunk.sumOfPageLength);
    }
}