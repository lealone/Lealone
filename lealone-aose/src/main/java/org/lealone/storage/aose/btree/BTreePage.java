/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.storage.aose.btree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.lealone.common.compress.Compressor;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.net.NetEndpoint;
import org.lealone.storage.LeafPageMovePlan;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.aose.btree.PageOperations.TmpNodePage;
import org.lealone.storage.fs.FileStorage;

//所有的子类都不是多线程安全的，但是设计层面会保证对每个Page的更新都只由一个线程负责，
//每个Page对应一个PageOperationHandler，由它处理对Page产生的操作。
public class BTreePage {

    public static class DynamicInfo {
        public final State state;
        public final BTreePage redirect;

        public DynamicInfo() {
            this(State.NORMAL, null);
        }

        public DynamicInfo(State state, BTreePage redirect) {
            this.state = state;
            this.redirect = redirect;
        }

        @Override
        public String toString() {
            return "DynamicInfo[" + state + "]";
        }
    }

    public static enum State {
        NORMAL,
        SPLITTING,
        SPLITTED;

        BTreePage redirect;

        public BTreePage getRedirect() {
            return redirect;
        }

        public void setRedirect(BTreePage redirect) {
            this.redirect = redirect;
        }
    }

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    protected final BTreeMap<?, ?> map;
    protected final PageOperationHandler handler;
    protected long pos;

    private boolean splitEnabled = true;
    volatile DynamicInfo dynamicInfo = new DynamicInfo();

    protected BTreePage(BTreeMap<?, ?> map) {
        this.map = map;
        if (isLeaf())
            handler = map.pohFactory.getPageOperationHandler();
        else if (isNode())
            handler = map.pohFactory.getNodePageOperationHandler();
        else
            handler = null;
    }

    public boolean isSplitEnabled() {
        return splitEnabled;
    }

    public void enableSplit() {
        splitEnabled = true;
    }

    public void disableSplit() {
        splitEnabled = false;
    }

    public PageOperationHandler getHandler() {
        return handler;
    }

    void addPageOperation(PageOperation po) {
        if (handler != null) {
            handler.handlePageOperation(po);
        }
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
    public BTreePage getChildPage(int index) {
        throw ie();
    }

    PageReference getChildPageReference(int index) {
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
    BTreePage split(int at) {
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
    public void setChild(int index, BTreePage c) {
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

    /**
     * Insert a child page into this node.
     * 
     * @param index the index
     * @param key the key
     * @param childPage the child page
     */
    public void insertNode(int index, Object key, BTreePage childPage) {
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
     * @param maxLength the maximum length
     */
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength) {
        read(buff, chunkId, offset, maxLength, false);
    }

    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        throw ie();
    }

    /**
    * Store the page and update the position.
    *
    * @param chunk the chunk
    * @param buff the target buffer
    * @return the position of the buffer just after the type
    */
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        throw ie();
    }

    void writeLeaf(DataBuffer buff, boolean remote) {
        throw ie();
    }

    static BTreePage readLeafPage(BTreeMap<?, ?> map, ByteBuffer page) {
        int type = page.get();
        if (type == PageUtils.PAGE_TYPE_LEAF)
            return BTreeLeafPage.readLeafPage(map, page);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            return BTreeRemotePage.readLeafPage(map, page);
        else
            throw DbException.throwInternalError("type: " + type);
    }

    /**
     * Store this page and all children that are changed, in reverse order, and
     * update the position and the children.
     * 
     * @param chunk the chunk
     * @param buff the target buffer
     */
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
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
    public BTreePage copy() {
        throw ie();
    }

    /**
     * Remove the page.
     */
    public void removePage() {
        throw ie();
    }

    /**
     * Remove this page and all child pages.
     */
    void removeAllRecursive() {
        throw ie();
    }

    String getPrettyPageInfo(boolean readOffLinePage) {
        throw ie();
    }

    /**
     * Read a page.
     * 
     * @param fileStorage the file storage
     * @param pos the position
     * @param map the map
     * @param filePos the position in the file
     * @param maxPos the maximum position (the end of the chunk)
     * @return the page
     */
    static BTreePage read(FileStorage fileStorage, long pos, BTreeMap<?, ?> map, long filePos, long maxPos) {
        int maxLength = PageUtils.getPageMaxLength(pos);
        ByteBuffer buff = readPageBuff(fileStorage, maxLength, filePos, maxPos);
        int type = PageUtils.getPageType(pos);
        BTreePage p = create(map, type);
        p.pos = pos;
        int chunkId = PageUtils.getPageChunkId(pos);
        int offset = PageUtils.getPageOffset(pos);
        p.read(buff, chunkId, offset, maxLength);
        return p;
    }

    static ByteBuffer readPageBuff(FileStorage fileStorage, int maxLength, long filePos, long maxPos) {
        ByteBuffer buff;
        if (maxLength == PageUtils.PAGE_LARGE) {
            buff = fileStorage.readFully(filePos, 128);
            maxLength = buff.getInt();
        }
        maxLength = (int) Math.min(maxPos - filePos, maxLength);
        int length = maxLength;
        if (length < 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "Illegal page length {0} reading at {1}; max pos {2} ", length, filePos, maxPos);
        }
        buff = fileStorage.readFully(filePos, length);
        return buff;
    }

    private static BTreePage create(BTreeMap<?, ?> map, int type) {
        BTreePage p;
        if (type == PageUtils.PAGE_TYPE_LEAF)
            p = new BTreeLeafPage(map);
        else if (type == PageUtils.PAGE_TYPE_NODE)
            p = new BTreeNodePage(map);
        else if (type == PageUtils.PAGE_TYPE_COLUMN)
            p = new BTreeColumnPage(map);
        else if (type == PageUtils.PAGE_TYPE_REMOTE)
            p = new BTreeRemotePage(map);
        else
            throw DbException.throwInternalError("type: " + type);
        return p;
    }

    // 返回key所在的leaf page
    BTreePage binarySearchLeafPage(Object key) {
        BTreePage p = this;
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
    BTreePage gotoLeafPage(Object key) {
        BTreePage p = this;
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

    void readRemotePages() {
        throw ie();
    }

    void readRemotePagesRecursive() {
        throw ie();
    }

    void moveAllLocalLeafPages(String[] oldEndpoints, String[] newEndpoints) {
        throw ie();
    }

    void replicatePage(DataBuffer buff, NetEndpoint localEndpoint) {
        throw ie();
    }

    static BTreePage readReplicatedPage(BTreeMap<?, ?> map, ByteBuffer buff) {
        int type = buff.get();
        BTreePage p = create(map, type);
        int chunkId = 0;
        int offset = buff.position();
        p.read(buff, chunkId, offset, buff.limit());
        return p;
    }

    boolean isRemoteChildPage(int index) {
        return false;
    }

    boolean isNodeChildPage(int index) {
        return false;
    }

    boolean isLeafChildPage(int index) {
        return false;
    }

    Object getLastKey() {
        throw ie();
    }

    // test only
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

    public static BTreeLeafPage createLeaf(BTreeMap<?, ?> map, Object[] keys, Object[] values, long totalCount,
            int memory) {
        return BTreeLeafPage.create(map, keys, values, totalCount, memory);
    }

    public static BTreeNodePage createNode(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        return BTreeNodePage.create(map, keys, children, memory);
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

    static void checkPageLength(int chunkId, int pageLength, int maxLength) {
        if (pageLength > maxLength || pageLength < 4) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_FILE_CORRUPT,
                    "File corrupted in chunk {0}, expected page length 4..{1}, got {2}", chunkId, maxLength,
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

    void updateChunkAndCachePage(BTreeChunk chunk, int start, int pageLength, int type) {
        if (pos != 0) {
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page already stored");
        }
        pos = PageUtils.getPagePos(chunk.id, start, pageLength, type);
        chunk.pagePositions.add(pos);
        chunk.pageLengths.add(pageLength);
        chunk.sumOfPageLength += pageLength;
        chunk.pageCount++;

        map.getBTreeStorage().cachePage(pos, this, getMemory());

        if (chunk.sumOfPageLength > BTreeChunk.MAX_SIZE)
            throw DataUtils.newIllegalStateException(DataUtils.ERROR_WRITING_FAILED,
                    "Chunk too large, max size: {0}, current size: {1}", BTreeChunk.MAX_SIZE, chunk.sumOfPageLength);
    }
}
