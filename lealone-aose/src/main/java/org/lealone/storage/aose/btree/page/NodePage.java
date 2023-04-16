/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.chunk.Chunk;
import org.lealone.storage.aose.btree.page.PageOperations.TmpNodePage;

public class NodePage extends LocalPage {

    // 对子page的引用，数组长度比keys的长度多一个
    private PageReference[] children;

    NodePage(BTreeMap<?, ?> map) {
        super(map);
    }

    @Override
    public boolean isNode() {
        return true;
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
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        if (ref.page != null) {
            return ref.page;
        } else {
            Page p;
            if (ref.buff != null) {
                p = Page.read(map, ref.pos, ref.buff, ref.pageLength);
                map.getBTreeStorage().cachePage(pos, p, p.getMemory());
            } else {
                p = map.getBTreeStorage().readPage(ref.pos);
                ref.buff = p.buff;
                ref.pageLength = p.pageLength;
            }
            ref.replacePage(p);
            p.setRef(ref);
            p.setParentRef(getRef());
            return p;
        }
    }

    @Override
    NodePage split(int at) { // at对应的key只放在父节点中
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

        NodePage newPage = create(map, bKeys, bChildren, 0);
        recalculateMemory();
        return newPage;
    }

    @Override
    public long getTotalCount() {
        long totalCount = 0;
        for (PageReference x : children) {
            if (x.page != null)
                totalCount += x.page.getTotalCount();
        }
        return totalCount;
    }

    @Override
    void setAndInsertChild(int index, TmpNodePage tmpNodePage) {
        children = children.clone(); // 必须弄一份新的，否则影响其他线程
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

        tmpNodePage.left.page.setParentRef(getRef());
        tmpNodePage.right.page.setParentRef(getRef());
        addMemory(map.getKeyType().getMemory(tmpNodePage.key) + PageUtils.PAGE_MEMORY_CHILD);
    }

    @Override
    public void remove(int index) {
        if (keys.length > 0) // 删除最后一个children时，keys已经空了
            super.remove(index);
        addMemory(-PageUtils.PAGE_MEMORY_CHILD);
        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount - 1];
        DataUtils.copyExcept(children, newChildren, childCount, index);
        children = newChildren;
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength,
            boolean disableCheck) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        readCheckValue(buff, chunkId, offset, pageLength, disableCheck);

        int keyLength = DataUtils.readVarInt(buff);
        keys = new Object[keyLength];
        int type = buff.get();
        children = new PageReference[keyLength + 1];
        long[] p = new long[keyLength + 1];
        for (int i = 0; i <= keyLength; i++) {
            p[i] = buff.getLong();
        }
        for (int i = 0; i <= keyLength; i++) {
            int pageType = buff.get();
            if (pageType == 0)
                buff.getInt(); // replicationHostIds
            children[i] = new PageReference(null, p[i]);
        }
        buff = expandPage(buff, type, start, pageLength);

        map.getKeyType().read(buff, keys, keyLength);
        recalculateMemory();
    }

    /**
    * Store the page and update the position.
    *
    * @param chunk the chunk
    * @param buff the target buffer
    * @return the position of the buffer just after the type
    */
    private int write(Chunk chunk, DataBuffer buff) {
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
            if (children[i].isLeafPage()) {
                buff.put((byte) 0);
                buff.putInt(0); // replicationHostIds
            } else {
                buff.put((byte) 1);
            }
        }
        int compressStart = buff.position();
        map.getKeyType().write(buff, keys, keyLength);

        compressPage(buff, compressStart, type, typePos);

        int pageLength = buff.position() - start;
        buff.putInt(start, pageLength);
        int chunkId = chunk.id;

        writeCheckValue(buff, chunkId, start, pageLength, checkPos);

        updateChunkAndCachePage(chunk, start, pageLength, type);

        // cache again - this will make sure nodes stays in the cache
        // for a longer time
        map.getBTreeStorage().cachePage(pos, this, getMemory());

        removeIfInMemory();
        return typePos + 1;
    }

    private void writeChildrenPositions(DataBuffer buff) {
        for (int i = 0, len = keys.length; i <= len; i++) {
            buff.putLong(children[i].pos); // pos通常是个很大的long，所以不值得用VarLong
        }
    }

    @Override
    public void writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        if (pos != 0) {
            // already stored before
            return;
        }
        int patch = write(chunk, buff);
        for (int i = 0, len = children.length; i < len; i++) {
            Page p = children[i].page;
            if (p != null) {
                p.writeUnsavedRecursive(chunk, buff);
                // 不能 children[i] = new PageReference(p);
                // 要保证 children[i] 等于 p.ref
                children[i].pos = p.pos;
            }
        }
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
                    throw DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL,
                            "Page not written");
                }
                ref.page.writeEnd();
                children[i] = new PageReference(null, ref.pos);
            }
        }
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
    public NodePage copy() {
        return copy(true);
    }

    private NodePage copy(boolean removePage) {
        NodePage newPage = create(map, keys, children, getMemory());
        newPage.cachedCompare = cachedCompare;
        newPage.setParentRef(getParentRef());
        newPage.setRef(getRef());
        if (removePage) {
            // mark the old as deleted
            removePage();
        }
        return newPage;
    }

    @Override
    public void removeAllRecursive() {
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
                        Chunk c = map.getBTreeStorage().getChunk(pos);
                        int mem = c.getPageLength(pos);
                        map.getBTreeStorage().removePage(pos, mem);
                    } else {
                        map.getBTreeStorage().readPage(pos).removeAllRecursive();
                    }
                }
            }
        }
        removePage();
    }

    static NodePage create(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        NodePage p = new NodePage(map);
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
    protected void getPrettyPageInfoRecursive(StringBuilder buff, String indent, PrettyPageInfo info) {
        if (children != null) {
            buff.append(indent).append("children: ").append(keys.length + 1).append('\n');
            for (int i = 0, len = keys.length; i <= len; i++) {
                buff.append('\n');
                if (children[i].page != null) {
                    children[i].page.getPrettyPageInfoRecursive(indent + "  ", info);
                } else {
                    if (info.readOffLinePage) {
                        map.getBTreeStorage().readPage(children[i].pos)
                                .getPrettyPageInfoRecursive(indent + "  ", info);
                    } else {
                        buff.append(indent).append("  ");
                        buff.append("*** off-line *** ").append(children[i]).append('\n');
                    }
                }
            }
        }
    }
}
