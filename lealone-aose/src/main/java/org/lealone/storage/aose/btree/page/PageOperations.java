/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import java.util.concurrent.Callable;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperationHandler;

public abstract class PageOperations {

    private PageOperations() {
    }

    public static class CallableOperation implements PageOperation {
        private final Callable<?> callable;

        public CallableOperation(Callable<?> task) {
            callable = task;
        }

        @Override
        public void run() {
            try {
                callable.call();
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
    }

    public static class RunnableOperation implements PageOperation {
        private final Runnable runnable;

        public RunnableOperation(Runnable task) {
            runnable = task;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    // 只针对单Key的写操作，包括: Put、PutIfAbsent、Replace、Remove、Append
    public static abstract class SingleWrite<K, V, R> implements PageOperation {
        final BTreeMap<K, V> map;
        final K key;
        final AsyncHandler<AsyncResult<R>> asyncResultHandler;

        Page p; // 最终要操作的leaf page
        PageReference pRef;
        Object result;
        ChildOperation childOperation; // 如果不为null，说明需要进一步切割或删除page

        public SingleWrite(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
            this.map = map;
            this.key = key;
            this.asyncResultHandler = asyncResultHandler;
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            if (p == null) {
                // 事先定位到leaf page，当加轻量级锁失败后再次运行时不用再定位leaf page
                p = gotoLeafPage();
                pRef = p.getRef();
            }

            // 处理分布式场景
            if (p.isRemote() || p.getLeafPageMovePlan() != null) {
                writeRemote();
                return PageOperationResult.REMOTE_WRITTING;
            }

            // 页面发生了结构性变动，重新从root定位leaf page
            if (pRef.page.isNode() || pRef.isDataStructureChanged()) {
                p = null;
                // 不用递归调用，让调度器重试
                return PageOperationResult.RETRY;
            }

            if (childOperation != null) {
                return runChildOperation(currentHandler);
            }
            if (pRef.tryLock(currentHandler)) {
                if (pRef.page.isNode() || pRef.isDataStructureChanged()) {
                    p = null;
                    pRef.unlock();
                    return PageOperationResult.RETRY;
                }
                p = pRef.page; // 使用最新的page
                write(currentHandler);
                if (childOperation != null) {
                    return runChildOperation(currentHandler);
                } else {
                    return handleAsyncResult();
                }
            } else {
                return PageOperationResult.LOCKED;
            }
        }

        private void write(PageOperationHandler currentHandler) {
            int index;
            try {
                // p.map.acquireSharedLock();
                index = getKeyIndex();
                result = writeLocal(index);
            } finally {
                // p.map.releaseSharedLock();
            }

            // 看看当前leaf page是否需要进行切割
            // 当index<0时说明是要增加新值，其他操作不切割(暂时不考虑被更新的值过大，导致超过page size的情况)
            if (index < 0 && p.needSplit()) {
                childOperation = splitLeafPage(currentHandler, p);
            }
        }

        private PageOperationResult runChildOperation(PageOperationHandler currentHandler) {
            if (childOperation.run(currentHandler)) {
                childOperation = null;
                pRef.setDataStructureChanged(true);
                return handleAsyncResult();
            }
            return PageOperationResult.LOCKED;
        }

        @SuppressWarnings("unchecked")
        private PageOperationResult handleAsyncResult() {
            pRef.unlock();
            AsyncResult<R> ar = new AsyncResult<>();
            ar.setResult((R) result);
            asyncResultHandler.handle(ar);
            return PageOperationResult.SUCCEEDED;
        }

        // 这里的index是key所在的leaf page的索引，
        // 可能是新增的key所要插入的index，也可能是将要修改或删除的index
        protected abstract Object writeLocal(int index);

        // 在分布式场景，当前leaf page已经被移到其他节点了
        protected abstract void writeRemote();

        protected void insertLeaf(int index, V value) {
            index = -index - 1;
            p = p.copyLeaf(index, key, value); // copy之后Ref还是一样的
            p.getRef().replacePage(p);
            map.setMaxKey(key);
        }

        protected void markDirtyPages() {
            p.markDirtyRecursive();
        }

        // 允许子类覆盖，比如Append操作可以做自己的特殊优化
        protected Page gotoLeafPage() {
            return map.gotoLeafPage(key, true);
        }

        protected int getKeyIndex() {
            return p.binarySearch(key);
        }
    }

    public static class Put<K, V, R> extends SingleWrite<K, V, R> {
        final V value;

        public Put(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
            super(map, key, asyncResultHandler);
            this.value = value;
        }

        @Override
        protected Object writeLocal(int index) {
            p.markDirty(true);
            // markDirtyPages();
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                return p.setValue(index, value);
            }
        }

        @Override
        protected void writeRemote() {
            map.putRemote(p, key, value, false, asyncResultHandler);
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<V>> asyncResultHandler) {
            super(map, key, value, asyncResultHandler);
        }

        @Override
        protected Object writeLocal(int index) {
            if (index < 0) {
                markDirtyPages();
                insertLeaf(index, value);
                return null;
            }
            return p.getValue(index);
        }

        @Override
        protected void writeRemote() {
            map.putRemote(p, key, value, true, asyncResultHandler);
        }
    }

    public static class Append<K, V> extends Put<K, V, K> {

        public Append(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<K>> asyncResultHandler) {
            super(map, key, value, asyncResultHandler);
        }

        @Override
        protected Object writeLocal(int index) {
            markDirtyPages();
            insertLeaf(index, value);
            return key;
        }

        @Override
        protected Page gotoLeafPage() { // 直接定位到最后一页
            Page p = map.getRootPage();
            while (true) {
                if (p.isLeaf()) {
                    return p;
                }
                p = p.getChildPage(map.getChildPageCount(p) - 1);
            }
        }

        @Override
        protected int getKeyIndex() {
            return -(p.getKeyCount() + 1);
        }

        @Override
        protected void writeRemote() {
            map.appendRemote(p, value, asyncResultHandler);
        }
    }

    public static class Replace<K, V> extends Put<K, V, Boolean> {
        final V oldValue;

        public Replace(BTreeMap<K, V> map, K key, V oldValue, V newValue,
                AsyncHandler<AsyncResult<Boolean>> asyncResultHandler) {
            super(map, key, newValue, asyncResultHandler);
            this.oldValue = oldValue;
        }

        @Override
        protected Boolean writeLocal(int index) {
            // 对应的key不存在，直接返回false
            if (index < 0) {
                return Boolean.FALSE;
            }
            Object old = p.getValue(index);
            if (map.areValuesEqual(old, oldValue)) {
                markDirtyPages();
                p.setValue(index, value);
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }

        @Override
        protected void writeRemote() {
            map.replaceRemote(p, key, oldValue, value, asyncResultHandler);
        }
    }

    public static class Remove<K, V> extends SingleWrite<K, V, V> {

        public Remove(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<V>> asyncResultHandler) {
            super(map, key, asyncResultHandler);
        }

        @Override
        protected Object writeLocal(int index) {
            if (index < 0) {
                return null;
            }
            markDirtyPages();
            Object oldValue = p.getValue(index);
            Page oldRootPage = p.map.getRootPage();
            Page newPage = p.copy(); // 删除元素需要先copy，否则会产生get和remove的并发问题
            newPage.remove(index);
            p.getRef().replacePage(newPage);
            if (newPage.isEmpty() && p != oldRootPage) { // 删除leaf page，但是root leaf page除外
                childOperation = new RemoveChild(p, key);
            }
            return oldValue;
        }

        @Override
        protected void writeRemote() {
            map.removeRemote(p, key, asyncResultHandler);
        }
    }

    private static interface ChildOperation {
        public boolean run(PageOperationHandler currentHandler);
    }

    // 这个类不处理root leaf page被切割的场景，在执行Put操作时已经直接处理，
    // 也就是说此时的btree至少有两层
    private static class AddChild implements ChildOperation {
        TmpNodePage tmpNodePage;
        int count;

        public AddChild(TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
        }

        @Override
        public boolean run(PageOperationHandler currentHandler) {
            return insertChildren(currentHandler, tmpNodePage) && count == 0;
        }

        private boolean insertChildren(PageOperationHandler currentHandler, TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
            Page parent = tmpNodePage.old.getParentRef().page;
            Page old = parent;
            if (!old.getRef().tryLock(currentHandler))
                return false;
            count++;
            PageReference parentRef = parent.getRef();
            int index = parent.getPageIndex(tmpNodePage.key);
            parent = parent.copy();
            parent.setAndInsertChild(index, tmpNodePage);
            parentRef.replacePage(parent);

            // 先看看父节点是否需要切割
            if (parent.needSplit()) {
                // node page的切割直接由单一的node page处理器处理，不会产生并发问题
                TmpNodePage tmp = splitPage(parent);
                for (PageReference ref : tmp.left.page.getChildren()) {
                    if (ref.page != null)
                        ref.page.setParentRef(tmp.left.page.getRef());
                }
                for (PageReference ref : tmp.right.page.getChildren()) {
                    if (ref.page != null)
                        ref.page.setParentRef(tmp.right.page.getRef());
                }
                // 如果是root node page，那么直接替换
                if (parent.getParentRef() == null) {
                    tmp.left.page.setParentRef(tmp.parent.getRef());
                    tmp.right.page.setParentRef(tmp.parent.getRef());
                    parent.map.newRoot(tmp.parent);
                } else {
                    insertChildren(currentHandler, tmp);
                }
            } else {
                // 如果是root node page，那么直接替换
                if (parent.getParentRef() == null)
                    parent.map.newRoot(parent);
            }
            count--;
            old.getRef().unlock();
            return true;
        }
    }

    // 不处理root leaf page的场景，Remove类那里已经保证不会删除root leaf page
    private static class RemoveChild implements ChildOperation {
        final Page old;
        final Object key;
        int count;

        public RemoveChild(Page old, Object key) {
            this.old = old;
            this.key = key;
        }

        @Override
        public boolean run(PageOperationHandler currentHandler) {
            Page root = old.map.getRootPage();
            if (!root.isNode()) {
                throw DbException.getInternalError();
            }
            Page p = remove(currentHandler, root, key);
            boolean ok = p != null;
            if (ok && count == 0) {
                if (p.isEmpty()) {
                    p = LeafPage.createEmpty(old.map);
                }
                old.map.newRoot(p);
                return true;
            }
            return false;
        }

        private Page remove(PageOperationHandler currentHandler, Page p, Object key) {
            int index = p.getPageIndex(key);
            Page c = p.getChildPage(index);
            Page cOld = c;
            if (c.isNode()) {
                // c = c.copy(); // leaf page不需要copy
                c = remove(currentHandler, c, key);
                if (c == null)
                    return null;
            }
            if (c.isNotEmpty()) {
                if (cOld != c)
                    cOld.getRef().replacePage(c);
                // no change, or there are more nodes
                // p.setChild(index, c);
            } else {
                Page old = p;
                if (!old.getRef().tryLock(currentHandler))
                    return null;
                count++;
                p = p.copy();
                p.remove(index);
                if (c.isLeaf()) {
                    old.map.fireLeafPageRemove(c.getRef().pageKey, c);
                }
                old.getRef().replacePage(p);
                count--;
                old.getRef().unlock();
            }
            return p;
        }
    }

    public static class TmpNodePage {
        final Page parent;
        final Page old;
        final PageReference left;
        final PageReference right;
        final Object key;

        public TmpNodePage(Page parent, Page old, PageReference left, PageReference right, Object key) {
            this.parent = parent;
            this.old = old;
            this.left = left;
            this.right = right;
            this.key = key;
        }
    }

    private static AddChild splitLeafPage(PageOperationHandler currentHandler, Page p) {
        // 第一步:
        // 切开page，得到一个临时的父节点和两个新的leaf page
        // 临时父节点只能通过被切割的page重定向访问
        TmpNodePage tmp = splitPage(p);

        // 第二步:
        // 如果是对root leaf page进行切割，因为当前只有一个线程在处理，所以直接替换root即可，这是安全的
        if (p == p.map.getRootPage()) {
            tmp.left.page.setParentRef(tmp.parent.getRef());
            tmp.right.page.setParentRef(tmp.parent.getRef());
            p.map.newRoot(tmp.parent);
            return null;
        }

        tmp.left.page.setParentRef(p.getParentRef());
        tmp.right.page.setParentRef(p.getParentRef());

        // 第三步:
        // 对于分布式场景，通知发生切割了，需要选一个leaf page来移动
        p.map.fireLeafPageSplit(tmp.key);

        // 第四步:
        // 创建新任务，准备放入父节点中
        return new AddChild(tmp);
    }

    private static TmpNodePage splitPage(Page p) {
        // 注意: 在这里被切割的页面可能是node page或leaf page
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        // 切割前必须copy当前被切割的页面，否则其他读线程可能读到切割过程中不一致的数据
        Page old = p;
        p = p.copy();
        // 对页面进行切割后，会返回右边的新页面，而copy后的当前被切割页面变成左边的新页面
        Page rightChildPage = p.split(at);
        Page leftChildPage = p;
        PageReference leftRef = new PageReference(leftChildPage, k, true);
        PageReference rightRef = new PageReference(rightChildPage, k, false);
        Object[] keys = { k };
        PageReference[] children = { leftRef, rightRef };
        Page parent = Page.createNode(p.map, keys, children, 0);
        PageReference parentRef = new PageReference(parent);
        parent.setRef(parentRef);
        // 它俩的ParentRef不在这里设置，调用者根据自己的情况设置
        leftChildPage.setRef(leftRef);
        rightChildPage.setRef(rightRef);
        return new TmpNodePage(parent, old, leftRef, rightRef, k);
    }
}
