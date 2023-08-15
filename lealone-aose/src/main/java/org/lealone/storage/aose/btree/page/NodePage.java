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
    public PageReference getChildPageReference(int index) {
        return children[index];
    }

    @Override
    public Page getChildPage(int index) {
        PageReference ref = children[index];
        if (ref.getParentRef() == null)
            ref.setParentRef(getRef());
        return ref.getOrReadPage();
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
    Page copyAndInsertChild(TmpNodePage tmpNodePage) {
        int index = getPageIndex(tmpNodePage.key);
        Object[] newKeys = new Object[keys.length + 1];
        DataUtils.copyWithGap(keys, newKeys, keys.length, index);
        newKeys[index] = tmpNodePage.key;

        PageReference[] newChildren = new PageReference[children.length + 1];
        DataUtils.copyWithGap(children, newChildren, children.length, index);
        newChildren[index] = tmpNodePage.left;
        newChildren[index + 1] = tmpNodePage.right;

        tmpNodePage.left.setParentRef(getRef());
        tmpNodePage.right.setParentRef(getRef());
        NodePage p = copy(newKeys, newChildren);
        p.addMemory(map.getKeyType().getMemory(tmpNodePage.key) + PageUtils.PAGE_MEMORY_CHILD, false);
        return p;
    }

    @Override
    public void remove(int index) {
        if (keys.length > 0) // 删除最后一个children时，keys已经空了
            super.remove(index);
        addMemory(-PageUtils.PAGE_MEMORY_CHILD, false);
        int childCount = children.length;
        PageReference[] newChildren = new PageReference[childCount - 1];
        DataUtils.copyExcept(children, newChildren, childCount, index);
        children = newChildren;
    }

    @Override
    public void read(ByteBuffer buff, int chunkId, int offset, int expectedPageLength) {
        int start = buff.position();
        int pageLength = buff.getInt();
        checkPageLength(chunkId, pageLength, expectedPageLength);
        readCheckValue(buff, chunkId, offset, pageLength);

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
            children[i] = new PageReference(map.getBTreeStorage(), p[i]);
            children[i].setParentRef(getRef());
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
    private long[] write(Chunk chunk, DataBuffer buff) {
        PagePos oldPagePos = posRef.get();
        int start = buff.position();
        int keyLength = keys.length;
        buff.putInt(0);
        int checkPos = buff.position();
        buff.putShort((short) 0).putVarInt(keyLength);
        int typePos = buff.position();
        int type = PageUtils.PAGE_TYPE_NODE;
        buff.put((byte) type);
        writeChildrenPositions(buff, null);
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

        writeCheckValue(buff, chunk, start, pageLength, checkPos);
        long pos = updateChunkAndPage(oldPagePos, chunk, start, pageLength, type);
        return new long[] { typePos + 1, pos };
    }

    private void writeChildrenPositions(DataBuffer buff, long[] positions) {
        if (positions == null) {
            for (int i = 0, len = keys.length; i <= len; i++) {
                buff.putLong(0);
            }
        } else {
            for (int i = 0, len = keys.length; i <= len; i++) {
                buff.putLong(positions[i]); // pos通常是个很大的long，所以不值得用VarLong
            }
        }
    }

    @Override
    public long writeUnsavedRecursive(Chunk chunk, DataBuffer buff) {
        beforeWrite();
        long ret[] = write(chunk, buff);
        int patch = (int) ret[0];
        long[] positions = new long[children.length];
        for (int i = 0, len = children.length; i < len; i++) {
            PageInfo pInfo = children[i].getPageInfo();
            Page p = pInfo.page;
            if (p != null && p.getPos() == 0) {
                long pos = p.writeUnsavedRecursive(chunk, buff);
                positions[i] = pos;
                pInfo.pos = pos;
            } else {
                positions[i] = pInfo.pos;
            }
        }
        int old = buff.position();
        buff.position(patch);
        writeChildrenPositions(buff, positions);
        buff.position(old);
        return ret[1];
    }

    @Override
    public int getRawChildPageCount() {
        return children.length;
    }

    @Override
    protected void recalculateMemory() {
        int mem = recalculateKeysMemory();
        mem += this.getRawChildPageCount() * PageUtils.PAGE_MEMORY_CHILD;
        addMemory(mem - memory, false);
    }

    @Override
    public NodePage copy() {
        return copy(keys, children);
    }

    private NodePage copy(Object[] keys, PageReference[] children) {
        NodePage newPage = create(map, keys, children, getMemory());
        super.copy(newPage);
        return newPage;
    }

    static NodePage create(BTreeMap<?, ?> map, Object[] keys, PageReference[] children, int memory) {
        NodePage p = new NodePage(map);
        // the position is 0
        p.keys = keys;
        p.children = children;
        if (memory == 0) {
            p.recalculateMemory();
        } else {
            p.addMemory(memory, false);
        }
        return p;
    }
}
