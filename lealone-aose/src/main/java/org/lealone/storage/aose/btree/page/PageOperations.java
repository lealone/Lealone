/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose.btree.page;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;
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
            // 这一步可以没有，但可以避免多线程在一个不再使用的page上加锁等待
            if (isPageChanged()) {
                p = null;
                return PageOperationResult.RETRY; // 不用递归调用，让调度器重试
            }

            if (pRef.tryLock(poHandler)) {
                // 这一步检查是必需的，不能在一个不再使用的page上面进行写操作
                if (isPageChanged()) {
                    p = null;
                    pRef.unlock();
                    return PageOperationResult.RETRY;
                }
                writeLocal(poHandler);
                return PageOperationResult.SUCCEEDED;
            } else {
                return PageOperationResult.LOCKED;
            }
        }

        @SuppressWarnings("unchecked")
        private void writeLocal(PageOperationHandler poHandler) {
            p = pRef.getOrReadPage(); // 使用最新的page
            int index = getKeyIndex();
            result = (R) writeLocal(index, poHandler);

            // 看看当前leaf page是否需要进行切割
            // 当index<0时说明是要增加新值，其他操作不切割(暂时不考虑被更新的值过大，导致超过page size的情况)
            if (index < 0 && p.needSplit()) {
                // 异步执行split操作，先尝试立刻执行，如果没有成功就加入等待队列
                asyncSplitPage(poHandler, pRef);
            }

            pRef.unlock(); // 快速释放锁，不用等处理结果
            if (resultHandler != null) {
                Session s = poHandler.getSession();
                if (s != null) {
                    s.addDirtyPage(p);
                }
                resultHandler.handle(new AsyncResult<>(result));
            }
        }

        // 这里的index是key所在的leaf page的索引，
        // 可能是新增的key所要插入的index，也可能是将要修改或删除的index
        protected abstract Object writeLocal(int index, PageOperationHandler poHandler);

        protected void insertLeaf(int index, V value) {
            index = -index - 1;
            p = p.copyAndInsertLeaf(index, key, value); // copy之后Ref还是一样的
            map.setMaxKey(key);
            pRef.replacePage(p);
        }

        // 以下两个API允许子类覆盖，比如Append操作可以做自己的特殊优化
        protected Page gotoLeafPage() {
            return map.gotoLeafPage(key);
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
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                Object obj = p.setValue(index, value);
                p.markDirtyBottomUp();
                return obj;
            }
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value,
                AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, value, resultHandler);
        }

        @Override
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            if (index < 0) {
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
        @SuppressWarnings("unchecked")
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            long k = map.incrementAndGetMaxKey();
            if (map.getKeyType() == ValueLong.type)
                key = (K) Long.valueOf(k);
            else
                key = (K) ValueLong.get(k);
            insertLeaf(index, value); // 执行insertLeaf的过程中已经把当前页标记为脏页了
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
        protected Boolean writeLocal(int index, PageOperationHandler poHandler) {
            // 对应的key不存在，直接返回false
            if (index < 0) {
                return Boolean.FALSE;
            }
            Object old = p.getValue(index);
            if (map.areValuesEqual(old, oldValue)) {
                p.setValue(index, value);
                p.markDirtyBottomUp();
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
        protected Object writeLocal(int index, PageOperationHandler poHandler) {
            if (index < 0) {
                return null;
            }
            Object oldValue = p.getValue(index);
            Page newPage = p.copy(); // 删除元素需要先copy，否则会产生get和remove的并发问题
            newPage.remove(index);
            pRef.replacePage(newPage);
            if (newPage.isEmpty() && !pRef.isRoot()) { // 删除leaf page，但是root leaf page除外
                asyncRemovePage(poHandler, pRef, key);
            }
            return oldValue;
        }
    }

    private static void asyncRemovePage(PageOperationHandler poHandler, PageReference pRef, Object key) {
        RemovePage rp = new RemovePage(pRef, key);
        if (rp.runLocked(poHandler) != PageOperationResult.SUCCEEDED)
            poHandler.handlePageOperation(rp);
    }

    private static void asyncSplitPage(PageOperationHandler poHandler, PageReference pRef) {
        SplitPage sp = new SplitPage(pRef);
        if (sp.runLocked(poHandler) != PageOperationResult.SUCCEEDED)
            poHandler.handlePageOperation(sp);
    }

    private static abstract class ChildOperation implements PageOperation {

        protected final PageReference pRef;

        public ChildOperation(PageReference pRef) {
            this.pRef = pRef;
        }

        @Override
        public PageOperationResult run(PageOperationHandler poHandler) {
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

        protected static boolean tryLockParentRef(PageReference pRef, PageOperationHandler poHandler) {
            PageReference parentRef = pRef.getParentRef();
            if (parentRef.isDataStructureChanged())
                return false;
            if (!parentRef.tryLock(poHandler))
                return false;
            if (parentRef.isDataStructureChanged()) {
                parentRef.unlock();
                return false;
            }
            if (pRef.getParentRef() != parentRef) {
                parentRef.unlock();
                return tryLockParentRef(pRef, poHandler);
            }
            return true;
        }
    }

    // 不处理root leaf page的场景，在Remove类那里已经保证不会删除root leaf page
    private static class RemovePage extends ChildOperation {

        private final Object key;

        public RemovePage(PageReference pRef, Object key) {
            super(pRef);
            this.key = key;
        }

        @Override
        protected PageOperationResult runLocked(PageOperationHandler poHandler) {
            Page p = pRef.getOrReadPage(); // 获得最新的
            // 如果是root node page，那么直接替换
            if (pRef.isRoot()) {
                p.map.newRoot(LeafPage.createEmpty(p.map));
            } else {
                if (!tryLockParentRef(pRef, poHandler))
                    return PageOperationResult.LOCKED;
                PageReference parentRef = pRef.getParentRef();
                Page parent = parentRef.getOrReadPage();
                int index = parent.getPageIndex(key);
                parent = parent.copy();
                parent.remove(index);
                parentRef.replacePage(parent);
                // 先看看父节点是否需要删除
                if (parent.isEmpty()) {
                    asyncRemovePage(poHandler, parentRef, key);
                }
                // 非root page被删除后，原有的ref被废弃
                pRef.setDataStructureChanged(true);
                parentRef.unlock();
            }
            p.markDirtyBottomUp();
            return PageOperationResult.SUCCEEDED;
        }
    }

    private static class SplitPage extends ChildOperation {

        public SplitPage(PageReference pRef) {
            super(pRef);
        }

        @Override
        protected PageOperationResult runLocked(PageOperationHandler poHandler) {
            Page p = pRef.getOrReadPage(); // 获得最新的
            // 如果是root page，那么直接替换
            if (pRef.isRoot()) {
                TmpNodePage tmpNodePage = splitPage(p);
                p.map.newRoot(tmpNodePage.parent);
                if (p.isNode())
                    setParentRef(tmpNodePage);
            } else {
                if (!tryLockParentRef(pRef, poHandler))
                    return PageOperationResult.LOCKED;
                TmpNodePage tmpNodePage = splitPage(p); // 先锁再切，避免做无用功
                PageReference parentRef = pRef.getParentRef();
                Page newParent = parentRef.getOrReadPage().copyAndInsertChild(tmpNodePage);
                parentRef.replacePage(newParent);
                // 先看看父节点是否需要切割
                if (newParent.needSplit()) {
                    asyncSplitPage(poHandler, parentRef);
                }
                // 非root page被切割后，原有的ref被废弃
                pRef.setDataStructureChanged(true);
                if (p.isNode())
                    setParentRef(tmpNodePage);
                parentRef.unlock();
            }
            p.markDirtyBottomUp();
            return PageOperationResult.SUCCEEDED;
        }

        private static TmpNodePage splitPage(Page p) {
            // 注意: 在这里被切割的页面可能是node page或leaf page
            int at = p.getKeyCount() / 2;
            Object k = p.getKey(at);
            // 切割前必须copy当前被切割的页面，否则其他读线程可能读到切割过程中不一致的数据
            p = p.copy();
            // 对页面进行切割后，会返回右边的新页面，而copy后的当前被切割页面变成左边的新页面
            Page rightChildPage = p.split(at);
            Page leftChildPage = p;
            PageReference leftRef = new PageReference(p.map.getBTreeStorage(), leftChildPage);
            PageReference rightRef = new PageReference(p.map.getBTreeStorage(), rightChildPage);
            Object[] keys = { k };
            PageReference[] children = { leftRef, rightRef };
            Page parent = NodePage.create(p.map, keys, children, 0);
            PageReference parentRef = new PageReference(p.map.getBTreeStorage(), parent);
            parent.setRef(parentRef);
            // 它俩的ParentRef不在这里设置，调用者根据自己的情况设置
            leftChildPage.setRef(leftRef);
            rightChildPage.setRef(rightRef);
            return new TmpNodePage(parent, leftRef, rightRef, k);
        }

        private static void setParentRef(TmpNodePage tmpNodePage) {
            PageReference lRef = tmpNodePage.left.getPage().getRef();
            PageReference rRef = tmpNodePage.right.getPage().getRef();
            for (PageReference ref : tmpNodePage.left.getPage().getChildren()) {
                if (ref.getPage() != null) // 没有加载的子节点直接忽略
                    ref.setParentRef(lRef);
            }
            for (PageReference ref : tmpNodePage.right.getPage().getChildren()) {
                if (ref.getPage() != null)
                    ref.setParentRef(rRef);
            }
        }
    }

    public static class TmpNodePage {
        final Page parent;
        final PageReference left;
        final PageReference right;
        final Object key;

        public TmpNodePage(Page parent, PageReference left, PageReference right, Object key) {
            this.parent = parent;
            this.left = left;
            this.right = right;
            this.key = key;
        }
    }
}
