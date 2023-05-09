/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.value.ValueLong;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.page.PageOperation;
import org.lealone.storage.page.PageOperation.PageOperationResult;
import org.lealone.storage.page.PageOperationHandler;

public abstract class PageOperations {

    private PageOperations() {
    }

    // 只针对单Key的写操作，包括: Put、PutIfAbsent、Replace、Remove、Append
    public static abstract class WriteOperation<K, V, R> implements PageOperation {

        final BTreeMap<K, V> map;
        K key; // 允许append操作设置
        AsyncHandler<AsyncResult<R>> resultHandler;

        Page p; // 最终要操作的leaf page
        PageReference pRef;
        R result;

        public WriteOperation(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<R>> resultHandler) {
            this.map = map;
            this.key = key;
            this.resultHandler = resultHandler;
        }

        // 可以延后设置
        public void setResultHandler(AsyncHandler<AsyncResult<R>> resultHandler) {
            this.resultHandler = resultHandler;
        }

        public AsyncHandler<AsyncResult<R>> getResultHandler() {
            return resultHandler;
        }

        public R getResult() {
            return result;
        }

        private boolean isPageChanged() {
            // leaf page被切割了或者root page从leaf page变成node page
            return pRef.isDataStructureChanged() || pRef.isNodePage();
        }

        @Override
        public PageOperationResult run(PageOperationHandler poHandler) {
            if (p == null) {
                // 事先定位到leaf page，当加轻量级锁失败后再次运行时不用再定位leaf page
                p = gotoLeafPage();
                pRef = p.getRef();
            }

            // 页面发生了结构性变动，重新从root定位leaf page
            if (isPageChanged()) {
                p = null;
                // 不用递归调用，让调度器重试
                return PageOperationResult.RETRY;
            }

            if (pRef.tryLock(poHandler)) {
                if (isPageChanged()) {
                    p = null;
                    pRef.unlock();
                    return PageOperationResult.RETRY;
                }
                write(poHandler);
                return PageOperationResult.SUCCEEDED;
            } else {
                return PageOperationResult.LOCKED;
            }
        }

        @SuppressWarnings("unchecked")
        private void write(PageOperationHandler poHandler) {
            p = pRef.page; // 使用最新的page
            int index = getKeyIndex();
            result = (R) writeLocal(index, poHandler);

            // 看看当前leaf page是否需要进行切割
            // 当index<0时说明是要增加新值，其他操作不切割(暂时不考虑被更新的值过大，导致超过page size的情况)
            if (index < 0 && p.needSplit()) {
                // 异步执行split操作，先尝试立刻执行，如果没有成功就加入等待队列
                asyncSplitPage(poHandler, p);
            }

            pRef.unlock(); // 快速释放锁，不用等处理结果
            if (resultHandler != null)
                resultHandler.handle(new AsyncResult<>(result));
        }

        // 这里的index是key所在的leaf page的索引，
        // 可能是新增的key所要插入的index，也可能是将要修改或删除的index
        protected abstract Object writeLocal(int index, PageOperationHandler poHandler);

        protected void insertLeaf(int index, V value) {
            index = -index - 1;
            p = p.copyLeaf(index, key, value); // copy之后Ref还是一样的
            p.getRef().replacePage(p);
            map.setMaxKey(key);
        }

        protected void markDirtyPages() {
            p.markDirtyRecursive();
        }

        // 一些像Put这样的操作可以一边定位leaf page一边把父节点标记为脏页
        // 还有一些像Remove这类操作就不需要，因为元素可能不存在
        protected abstract boolean isMarkDirtyEnabled();

        // 以下两个API允许子类覆盖，比如Append操作可以做自己的特殊优化
        protected Page gotoLeafPage() {
            return map.gotoLeafPage(key, isMarkDirtyEnabled());
        }

        protected int getKeyIndex() {
            return p.binarySearch(key);
        }
    }

    public static class Put<K, V, R> extends WriteOperation<K, V, R> {

        final V value;

        public Put(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<R>> resultHandler) {
            super(map, key, resultHandler);
            this.value = value;
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return true;
        }

        @Override
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            p.markDirty(true);
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                return p.setValue(index, value);
            }
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value,
                AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, value, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            if (index < 0) {
                markDirtyPages();
                insertLeaf(index, value);
                return null;
            }
            return p.getValue(index);
        }
    }

    public static class Append<K, V> extends Put<K, V, K> {

        public Append(BTreeMap<K, V> map, V value, AsyncHandler<AsyncResult<K>> resultHandler) {
            super(map, null, value, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() { // 已经自己实现gotoLeafPage了，所以不可能调用到
            throw DbException.getInternalError();
        }

        @Override
        protected Page gotoLeafPage() { // 直接定位到最后一页
            Page p = map.getRootPage();
            while (true) {
                if (p.isLeaf()) {
                    return p;
                }
                p.markDirty();
                p = p.getChildPage(map.getChildPageCount(p) - 1);
            }
        }

        @Override
        protected int getKeyIndex() {
            return -(p.getKeyCount() + 1);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            key = (K) ValueLong.get(map.incrementAndGetMaxKey());
            p.markDirty(true);
            insertLeaf(index, value);
            return key;
        }
    }

    public static class Replace<K, V> extends Put<K, V, Boolean> {

        private final V oldValue;

        public Replace(BTreeMap<K, V> map, K key, V oldValue, V newValue,
                AsyncHandler<AsyncResult<Boolean>> resultHandler) {
            super(map, key, newValue, resultHandler);
            this.oldValue = oldValue;
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Boolean writeLocal(int index, PageOperationHandler poHandler) {
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
    }

    public static class Remove<K, V> extends WriteOperation<K, V, V> {

        public Remove(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, resultHandler);
        }

        @Override
        protected boolean isMarkDirtyEnabled() {
            return false;
        }

        @Override
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            if (index < 0) {
                return null;
            }
            markDirtyPages();
            Object oldValue = p.getValue(index);
            Page oldRootPage = map.getRootPage();
            Page newPage = p.copy(); // 删除元素需要先copy，否则会产生get和remove的并发问题
            newPage.remove(index);
            p.getRef().replacePage(newPage);
            if (newPage.isEmpty() && p != oldRootPage) { // 删除leaf page，但是root leaf page除外
                asyncRemovePage(poHandler, p, key);
            }
            return oldValue;
        }
    }

    private static void asyncRemovePage(PageOperationHandler poHandler, Page page, Object key) {
        RemovePage rp = new RemovePage(page, key);
        if (rp.runLocked(poHandler) != PageOperationResult.SUCCEEDED)
            poHandler.handlePageOperation(rp);
    }

    private static void asyncSplitPage(PageOperationHandler poHandler, Page page) {
        SplitPage sp = new SplitPage(page);
        if (sp.runLocked(poHandler) != PageOperationResult.SUCCEEDED)
            poHandler.handlePageOperation(sp);
    }

    private static abstract class ChildOperation implements PageOperation {

        protected final Page page;

        public ChildOperation(Page page) {
            this.page = page;
        }

        @Override
        public PageOperationResult run(PageOperationHandler poHandler) {
            PageReference pRef = page.getRef();
            if (pRef.isDataStructureChanged()) // 忽略掉
                return PageOperationResult.SUCCEEDED;
            if (!pRef.tryLock(poHandler))
                return PageOperationResult.LOCKED;
            if (pRef.isDataStructureChanged()) { // 需要再判断一次
                pRef.unlock();
                return PageOperationResult.SUCCEEDED;
            }
            PageOperationResult res = runLocked(poHandler);
            pRef.unlock();
            return res;
        }

        protected abstract PageOperationResult runLocked(PageOperationHandler poHandler);

        protected static boolean tryLockParentRef(Page page, PageOperationHandler poHandler) {
            PageReference parentRef = page.getParentRef();
            if (parentRef.isDataStructureChanged())
                return false;
            if (!parentRef.tryLock(poHandler))
                return false;
            if (parentRef.isDataStructureChanged()) {
                parentRef.unlock();
                return false;
            }
            if (page.getParentRef() != parentRef) {
                parentRef.unlock();
                return tryLockParentRef(page, poHandler);
            }
            return true;
        }
    }

    // 不处理root leaf page的场景，在Remove类那里已经保证不会删除root leaf page
    private static class RemovePage extends ChildOperation {

        private final Object key;

        public RemovePage(Page page, Object key) {
            super(page);
            this.key = key;
        }

        @Override
        protected PageOperationResult runLocked(PageOperationHandler poHandler) {
            PageReference pRef = page.getRef();
            Page p = pRef.getPage(); // 获得最新的
            // 如果是root node page，那么直接替换
            if (p.getParentRef() == null) {
                p.map.newRoot(LeafPage.createEmpty(page.map));
            } else {
                if (!tryLockParentRef(p, poHandler))
                    return PageOperationResult.LOCKED;
                PageReference parentRef = p.getParentRef();
                Page parent = parentRef.getPage();
                int index = parent.getPageIndex(key);
                parent = parent.copy();
                parent.remove(index);
                parentRef.replacePage(parent);
                // 先看看父节点是否需要切割
                if (parent.isEmpty()) {
                    asyncRemovePage(poHandler, parent, key);
                }
                // 非root page被切割后，原有的ref被废弃
                if (p.map.getRootPageRef() != pRef)
                    pRef.setDataStructureChanged(true);
                parentRef.unlock();
            }
            return PageOperationResult.SUCCEEDED;
        }
    }

    private static class SplitPage extends ChildOperation {

        public SplitPage(Page page) {
            super(page);
        }

        @Override
        protected PageOperationResult runLocked(PageOperationHandler poHandler) {
            PageReference pRef = page.getRef();
            Page p = pRef.getPage(); // 获得最新的
            TmpNodePage tmpNodePage;
            // 如果是root page，那么直接替换
            if (p.getParentRef() == null) {
                tmpNodePage = splitPage(p);
                p.map.newRoot(tmpNodePage.parent);
                if (p.isNode())
                    setParentRef(tmpNodePage);
            } else {
                if (!tryLockParentRef(p, poHandler))
                    return PageOperationResult.LOCKED;
                tmpNodePage = splitPage(p); // 先锁再切，避免做无用功
                PageReference parentRef = p.getParentRef();
                Page newParent = parentRef.getPage().copyAndInsertChild(tmpNodePage);
                parentRef.replacePage(newParent);
                // 先看看父节点是否需要切割
                if (newParent.needSplit()) {
                    asyncSplitPage(poHandler, newParent);
                }
                // 非root page被切割后，原有的ref被废弃
                if (p.map.getRootPageRef() != pRef)
                    pRef.setDataStructureChanged(true);
                if (p.isNode())
                    setParentRef(tmpNodePage);
                parentRef.unlock();
            }
            return PageOperationResult.SUCCEEDED;
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
            PageReference leftRef = new PageReference(leftChildPage);
            PageReference rightRef = new PageReference(rightChildPage);
            Object[] keys = { k };
            PageReference[] children = { leftRef, rightRef };
            Page parent = NodePage.create(p.map, keys, children, 0);
            PageReference parentRef = new PageReference(parent);
            parent.setRef(parentRef);
            // 它俩的ParentRef不在这里设置，调用者根据自己的情况设置
            leftChildPage.setRef(leftRef);
            rightChildPage.setRef(rightRef);
            return new TmpNodePage(parent, old, leftRef, rightRef, k);
        }

        private static void setParentRef(TmpNodePage tmpNodePage) {
            PageReference lRef = tmpNodePage.left.page.getRef();
            PageReference rRef = tmpNodePage.right.page.getRef();
            for (PageReference ref : tmpNodePage.left.page.getChildren()) {
                if (ref.page != null) // 没有加载的子节点直接忽略
                    ref.page.setParentRef(lRef);
            }
            for (PageReference ref : tmpNodePage.right.page.getChildren()) {
                if (ref.page != null)
                    ref.page.setParentRef(rRef);
            }
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
}
