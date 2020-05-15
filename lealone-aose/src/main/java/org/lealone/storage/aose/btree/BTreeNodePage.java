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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.RunMode;
import org.lealone.net.NetNode;
import org.lealone.storage.PageKey;
import org.lealone.storage.aose.btree.PageOperations.CallableOperation;
import org.lealone.storage.aose.btree.PageOperations.TmpNodePage;

public class BTreeNodePage extends BTreeLocalPage {

    // 对子page的引用，数组长度比keys的长度多一个
    private PageReference[] children;

    BTreeNodePage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isNode() {
        return true;
    }

    @Override
    boolean isRemoteChildPage(int index) {
        return getChildPageReference(index).isRemotePage();
    }

    @Override
    boolean isNodeChildPage(int index) {
        return getChildPageReference(index).isNodePage();
    }

    @Override
    boolean isLeafChildPage(int index) {
        return getChildPageReference(index).isLeafPage();
    }

    @Override
    public boolean isEmpty() {
        return children == null || children.length == 0;
    }

    @Override
    public PageReference[] getChildren() {
        return children;
    }

    @Override
    PageReference getChildPageReference(int index) {
        return children[index];
    }

    @Override
    public BTreePage getChildPage(int index) {
        PageReference ref = children[index];
        if (ref.page != null) {
            return ref.page;
        } else {
            BTreePage p = map.btreeStorage.readPage(ref, ref.pos);
            ref.replacePage(p);
            return p;
        }
    }

    @Override
    BTreeNodePage split(int at) { // at对应的key只放在父节点中
        int a = at, b = keys.length - a;
        Object[] aKeys = new Object[a];
        Object[] bKeys = new Object[b - 1];
        System.arraycopy(keys, 0, aKeys, 0, a);
        System.arraycopy(keys, a + 1, bKeys, 0, b - 1);
        keys = aKeys;

        // children的长度要比keys的长度多1并且右边所有leaf的key都大于或等于at下标对应的key
        PageReference[] aChildren = new PageReference[a + 1];
        PageReference[] bChildren = new PageReference[b];
        System.arraycopy(children, 0, aChildren, 0, a + 1);
        System.arraycopy(children, a + 1, bChildren, 0, b);
        children = aChildren;

        BTreeNodePage newPage = create(map, bKeys, bChildren, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    @Deprecated
    public long getTotalCount() {
        long totalCount = 0;
        for (PageReference x : children) {
            if (x.page != null)
                totalCount += x.page.getTotalCount();
        }
        return totalCount;
    }

    @Override
    public void setChild(int index, BTreePage c) {
        Object key;
        boolean first;
        if (keys.length > 0) {
            int keyIndex = index > 0 ? index - 1 : 0;
            key = keys[keyIndex];
            first = index == 0;
        } else {
            key = children[index].pageKey.key;
            first = true;
        }
        if (c == null) {
            // this is slightly slower:
            // children = Arrays.copyOf(children, children.length);
            children = children.clone();
            PageReference ref = new PageReference(null, 0, key, first);
            children[index] = ref;
        } else if (c != children[index].page || c.getPos() != children[index].pos) {
            children = children.clone();
            PageReference ref = new PageReference(c, key, first);
            children[index] = ref;
        } else {
            PageReference ref = new PageReference(c, key, first);
            children[index] = ref;
        }
    }

    @Override
    public void setChild(int index, PageReference ref) {
        children[index] = ref;
    }

    @Override
    void setAndInsertChild(int index, TmpNodePage tmpNodePage) {
        children = children.clone();
        children[index] = tmpNodePage.right;
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = tmpNodePage.key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = tmpNodePage.left;
        children = newChildren;

        PageReference parentRef = new PageReference(this);
        tmpNodePage.left.page.parentRef = parentRef;
        tmpNodePage.right.page.parentRef = parentRef;
        addMemory(map.getKeyType().getMemory(tmpNodePage.key) + PageUtils.PAGE_MEMORY_CHILD);
    }

    @Override
    public void insertNode(int index, Object key, BTreePage childPage) {
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = key;
        keys = newKeys;

        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount + 1];
        DataUtils.copyWithGap(children, newChildren, childCount, index);
        newChildren[index] = new PageReference(childPage, key, index == 0);
        children = newChildren;

        PageReference parentRef = new PageReference(this);
        childPage.parentRef = parentRef;
        addMemory(map.getKeyType().getMemory(key) + PageUtils.PAGE_MEMORY_CHILD);
    }

    @Override
    public void remove(int index) {
        super.remove(index);
        addMemory(-PageUtils.PAGE_MEMORY_CHILD);
        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount - 1];
        DataUtils.copyExcept(children, newChildren, childCount, index);
        children = newChildren;
    }

    @Override
    void read(ByteBuffer buff, int chunkId, int offset, int maxLength, boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, maxLength);

        int oldLimit = buff.limit();
        buff.limit(start + pageLength);

        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        children = new PageReference[keyLength + 1];
        long[] p = new long[keyLength + 1];
        for (int i = 0; i <= keyLength; i++) {
            p[i] = buff.getLong();
        }
        List<String> defaultReplicationHostIds = map.getDatabase() == null ? null
                : Arrays.asList(map.getDatabase().getHostIds());
        for (int i = 0; i <= keyLength; i++) {
            int pageType = buff.get();
            boolean isRemotePage = pageType == 2;
            if (isRemotePage) {
                children[i] = PageReference.createRemotePageReference();
                List<String> replicationHostIds = readReplicationHostIds(buff);
                if (replicationHostIds == null) {
                    replicationHostIds = defaultReplicationHostIds;
                }
                children[i].replicationHostIds = replicationHostIds;
            } else {
                List<String> replicationHostIds = null;
                if (pageType == 0) {
                    replicationHostIds = readReplicationHostIds(buff);
                    if (replicationHostIds == null) {
                        replicationHostIds = defaultReplicationHostIds;
                    }
                }
                children[i] = new PageReference(null, p[i]);
                children[i].replicationHostIds = replicationHostIds; // node page的replicationHostIds为null
            }
        }
        ByteBuffer oldBuff = buff;
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        setChildrenPageKeys();
        recalculateMemory();
        oldBuff.limit(oldLimit);
    }

    private void setChildrenPageKeys() {
        if (children != null && keys != null) {
            int keyLength = keys.length;
            children[0].setPageKey(keys[0], true);
            for (int i = 0; i < keyLength; i++) {
                children[i + 1].setPageKey(keys[i], false);
            }
        }
    }

    @Override
    int write(BTreeChunk chunk, DataBuffer buff, boolean replicatePage) {
        int start = buff.position();
        int keyLength = keys.length;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        int type = PageUtils.PAGE_TYPE_NODE;
        buff.put((byte) type);
        writeChildrenPositions(buff);
        for (int i = 0; i <= keyLength; i++) {
            if (children[i].isRemotePage()) {
                buff.put((byte) 2);
                writeReplicationHostIds(children[i].replicationHostIds, buff);
            } else {
                if (children[i].isLeafPage()) {
                    buff.put((byte) 0);
                    writeReplicationHostIds(children[i].replicationHostIds, buff);
                } else {
                    buff.put((byte) 1);
                }
            }
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);

        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        if (replicatePage) {
            return typePos + 1;
        }

        updateChunkAndCachePage(chunk, start, pageLength, type);

        // cache again - this will make sure nodes stays in the cache
        // for a longer time
        map.getBTreeStorage().cachePage(pos, this, getMemory());

        if (removedInMemory) {
            // if the page was removed _before_ the position was assigned, we
            // need to mark it removed here, so the fields are updated
            // when the next chunk is stored
            map.getBTreeStorage().removePage(pos, memory);
        }
        return typePos + 1;
    }

    private void writeChildrenPositions(DataBuffer buff) {
        for (int i = 0, len = keys.length; i <= len; i++) {
            buff.putLong(children[i].pos); // pos通常是个很大的long，所以不值得用VarLong
        }
    }

    @Override
    void writeUnsavedRecursive(BTreeChunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff, false);
        for (int i = 0, len = children.length; i < len; i++) {
            BTreePage p = children[i].page;
            if (p != null) {
                p.writeUnsavedRecursive(chunk, buff);
                children[i] = new PageReference(p);
            }
        }
        setChildrenPageKeys();
        int old = buff.position();
        buff.position(patch);
        writeChildrenPositions(buff);
        buff.position(old);
    }

    @Override
    void writeEnd() {
        for (int i = 0, len = children.length; i < len; i++) {
            PageReference ref = children[i];
            if (ref.page != null) {
                if (ref.page.getPos() == 0) {
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos);
                children[i].replicationHostIds = ref.page.getReplicationHostIds();
            }
        }
        setChildrenPageKeys();
    }

    @Override
    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        mem += this.getRawChildPageCount() * PageUtils.PAGE_MEMORY_CHILD;
        addMemory(mem - memory);
    }

    @Override
    public BTreeNodePage copy() {
        return copy(true);
    }

    private BTreeNodePage copy(boolean removePage) {
        BTreeNodePage newPage = create(map, keys, children, getMemory());
        newPage.cachedCompare = cachedCompare;
        if (removePage) {
            // mark the old as deleted
            removePage();
        }
        return newPage;
    }

    @Override
    void removeAllRecursive() {
        if (children != null) {
            // TODO 消除这些难理解的规则
            // 不能直接使用getRawChildPageCount， RTreeMap这样的子类会返回getRawChildPageCount() - 1
            for (int i = 0, size = map.getChildPageCount(this); i < size; i++) {
                PageReference ref = children[i];
                if (ref.page != null) {
                    ref.page.removeAllRecursive();
                } else {
                    long pos = children[i].pos;
                    int type = PageUtils.getPageType(pos);
                    if (type == PageUtils.PAGE_TYPE_LEAF) {
                        int mem = PageUtils.getPageMaxLength(pos);
                        map.btreeStorage.removePage(pos, mem);
                    } else {
                        map.btreeStorage.readPage(pos).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    static BTreeNodePage create(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        BTreeNodePage p = new BTreeNodePage(map);
        // the position is 0
        p.keys = keys;
        p.children = children;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory);
        }
        return p;
    }

    @Override
    void readRemotePages() {
        for (int i = 0, len = children.length; i < len; i++) {
            final int index = i;
            Callable<BTreePage> task = new Callable<BTreePage>() {
                @Override
                public BTreePage call() throws Exception {
                    BTreePage p = getChildPage(index);
                    return p;
                }
            };
            map.pohFactory.addPageOperation(new CallableOperation(task));
        }
    }

    @Override
    void moveAllLocalLeafPages(String[] oldNodes, String[] newNodes, RunMode newRunMode) {
        Set<NetNode> candidateNodes = BTreeMap.getCandidateNodes(map.getDatabase(), newNodes);
        for (int i = 0, len = keys.length; i <= len; i++) {
            if (!children[i].isRemotePage()) {
                BTreePage p = getChildPage(i);
                if (p.isNode()) {
                    p.moveAllLocalLeafPages(oldNodes, newNodes, newRunMode);
                } else {
                    List<String> replicationHostIds = p.getReplicationHostIds();
                    Object key = i == len ? keys[i - 1] : keys[i];
                    if (replicationHostIds == null) {
                        oldNodes = new String[0];
                    } else {
                        oldNodes = new String[replicationHostIds.size()];
                        replicationHostIds.toArray(oldNodes);
                    }
                    PageKey pk = new PageKey(key, i == 0);
                    map.replicateOrMovePage(pk, p, this, i, oldNodes, false, candidateNodes, newRunMode);
                }
            }
        }
    }

    @Override
    void replicatePage(DataBuffer buff, NetNode localNode) {
        BTreeNodePage p = copy(false);
        // 这里不需要为PageReference生成PageKey，
        // 生成PageReference只是为了调用write时把子Page当成RemotePage
        int len = children.length;
        p.children = new PageReference[len];
        for (int i = 0; i < len; i++) {
            PageReference r = PageReference.createRemotePageReference();
            p.children[i] = r;
        }

        BTreeChunk chunk = new BTreeChunk(0);
        buff.put((byte) PageUtils.PAGE_TYPE_NODE);
        p.write(chunk, buff, true);
    }

    @Override
    protected void toString0(StringBuilder buff) {
        for (int i = 0, len = keys.length; i <= len; i++) {
            if (i > 0) {
                buff.append(" ");
            }
            if (children != null) {
                buff.append("[" + Long.toHexString(children[i].pos) + "] ");
            }
            if (i < keys.length) {
                buff.append(keys[i]);
            }
        }
    }

    @Override
    protected void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info) {
        if (children != null) {
            buff.append(indent).append("children: ").append(keys.length + 1).append('\n');
            for (int i = 0, len = keys.length; i <= len; i++) {
                buff.append('\n');
                if (children[i].page != null) {
                    children[i].page.getPrettyPageInfoRecursive(indent + "  ", info);
                } else {
                    if (info.readOffLinePage) {
                        map.btreeStorage.readPage(children[i].pos).getPrettyPageInfoRecursive(indent + "  ", info);
                    } else {
                        buff.append(indent).append("  ");
                        buff.append("*** off-line *** ").append(children[i]).append('\n');
                    }
                }
            }
        }
    }
}
