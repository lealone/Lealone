/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.aose.btree.page;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.lock.Lockable;
import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.session.InternalSession;
import com.lealone.storage.aose.btree.BTreeGC;
import com.lealone.storage.aose.btree.BTreeMap;
import com.lealone.storage.aose.btree.page.PageInfo.RemovedPageInfo;
import com.lealone.storage.aose.btree.page.PageInfo.SplittedPageInfo;
import com.lealone.storage.page.PageListener;
import com.lealone.storage.page.PageOperation;
import com.lealone.storage.page.PageOperation.PageOperationResult;

public abstract class PageOperations {

    private PageOperations() {
    }

    // 只针对单Key的写操作，包括: Put、PutIfAbsent、Remove、Append
    public static abstract class WriteOperation<K, V, R> implements PageOperation {

        final BTreeMap<K, V> map;
        K key; // 允许append操作设置
        AsyncHandler<AsyncResult<R>> resultHandler;
        R result;

        Page p; // 最终要操作的leaf page
        PageReference pRef;

        // 在标记脏页时
        // 如果pRef及它的父节点中的最新PageListener与pListener及它的parent不同
        // 说明被GC线程回收了需要重试
        PageListener pListener;

        InternalSession currentSession;

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

        public void setPageReference(PageReference ref) {
            pRef = ref;
            pListener = pRef.getPageListener();
        }

        @Override
        public InternalSession getSession() {
            return currentSession;
        }

        public void setSession(InternalSession session) {
            this.currentSession = session;
        }

        public R getResult() {
            return result;
        }

        private boolean isPageChanged() {
            // leaf page被切割了或者root page从leaf page变成node page
            return pRef.isDataStructureChanged() || pRef.isNodePage();
        }

        @Override
        public PageOperationResult run(InternalScheduler scheduler, boolean waitingIfLocked) {
            if (pRef == null) {
                // 先定位到leaf page，加轻量级锁失败后再次运行时不用再定位leaf page
                pRef = gotoLeafPage().getRef();
                pListener = pRef.getPageListener();
            }
            // 页面发生了结构性变动，也要重新从root定位leaf page
            if (isPageChanged())
                return retry(false);

            if (pRef.tryLock(scheduler, waitingIfLocked)) {
                p = pRef.getPage(); // 使用最新的
                // 如果被GC线程回收了需要重试
                // 这一步检查是必需的，不能在一个不再使用的page上面进行写操作
                if (p == null || isPageChanged())
                    return retry(true);

                try {
                    return writeLocal(scheduler);
                } catch (Throwable t) {
                    pRef.unlock();
                    if (resultHandler != null) {
                        resultHandler.handle(new AsyncResult<>(t));
                    } else {
                        throw DbException.convert(t);
                    }
                    return PageOperationResult.FAILED;
                }
            } else {
                return PageOperationResult.LOCKED;
            }
        }

        private PageOperationResult retry(boolean unlock) {
            if (unlock)
                pRef.unlock();
            pRef = null;
            return PageOperationResult.RETRY; // 不用递归调用，让调度器重试
        }

        @SuppressWarnings("unchecked")
        private PageOperationResult writeLocal(InternalScheduler scheduler) {
            currentSession = scheduler.getCurrentSession();
            int index = getKeyIndex();

            // 如果GC线程在写之前回收page了，要重试
            // 如果像PutIfAbsent这类操作不满足条件可以提前返回结果
            Object r = beforeWrite(index);
            if (r == PageOperationResult.SUCCEEDED) {
                result = (R) writeLocal(index, scheduler);

                // 看看当前leaf page是否需要进行切割
                // 当index<0时说明是要增加新值，其他操作不切割(暂时不考虑被更新的值过大，导致超过page size的情况)
                if (index < 0 && p.needSplit()) {
                    // 异步执行split操作，先尝试立刻执行，如果没有成功就加入等待队列
                    asyncSplitPage(scheduler, true, currentSession, pRef);
                }
            } else if (r == PageOperationResult.RETRY) {
                return retry(true);
            } else {
                result = (R) r; // 提前返回结果了
            }

            pRef.unlock(); // 快速释放锁，不用等处理结果

            if (resultHandler != null) {
                resultHandler.handle(new AsyncResult<>(result));
            }
            return PageOperationResult.SUCCEEDED;
        }

        protected Object beforeWrite(int index) {
            if (map.isInMemory() || pRef.markDirtyPage(pListener))
                return PageOperationResult.SUCCEEDED;
            else
                return PageOperationResult.RETRY;
        }

        // 这里的index是key所在的leaf page的索引，
        // 可能是新增的key所要插入的index，也可能是将要修改或删除的index
        protected abstract Object writeLocal(int index, InternalScheduler scheduler);

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
        protected Object writeLocal(int index, InternalScheduler scheduler) {
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                return p.setValue(index, value);
            }
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value, //
                AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, value, resultHandler);
        }

        @Override
        protected Object beforeWrite(int index) {
            if (index >= 0)
                return p.getValue(index); // 已经存在了，直接返回旧值即可
            else
                return super.beforeWrite(index);
        }

        @Override
        protected Object writeLocal(int index, InternalScheduler scheduler) {
            insertLeaf(index, value);
            return null;
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
        protected Object writeLocal(int index, InternalScheduler scheduler) {
            long k = map.incrementAndGetMaxKey();
            key = (K) map.getKeyType().getAppendKey(k, value);
            insertLeaf(index, value); // 执行insertLeaf的过程中已经把当前页标记为脏页了
            return key;
        }
    }

    public static class Remove<K, V> extends WriteOperation<K, V, V> {

        public Remove(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<V>> resultHandler) {
            super(map, key, resultHandler);
        }

        @Override
        protected Object beforeWrite(int index) {
            // 对应的key不存在，直接返回null
            if (index < 0)
                return null;
            else
                return super.beforeWrite(index);
        }

        @Override
        protected Object writeLocal(int index, InternalScheduler scheduler) {
            Object oldValue = p.getValue(index);
            Page newPage = p.copy(); // 删除元素需要先copy，否则会产生get和remove的并发问题
            newPage.remove(index);
            pRef.replacePage(newPage);
            if (newPage.isEmpty() && !pRef.isRoot()) { // 删除leaf page，但是root leaf page除外
                asyncRemovePage(scheduler, true, currentSession, pRef, key);
            }
            return oldValue;
        }
    }

    private static void asyncRemovePage(InternalScheduler scheduler, boolean waitingIfLocked,
            InternalSession session, PageReference pRef, Object key) {
        RemovePage rp = new RemovePage(session, pRef, key);
        if (rp.runLocked(scheduler, waitingIfLocked) != PageOperationResult.SUCCEEDED)
            scheduler.handlePageOperation(rp);
    }

    private static void asyncSplitPage(InternalScheduler scheduler, boolean waitingIfLocked,
            InternalSession session, PageReference pRef) {
        SplitPage sp = new SplitPage(session, pRef);
        if (sp.runLocked(scheduler, waitingIfLocked) != PageOperationResult.SUCCEEDED)
            scheduler.handlePageOperation(sp);
    }

    private static abstract class ChildOperation implements PageOperation {

        protected final PageReference pRef;
        protected PageListener oldPageListener;

        public ChildOperation(InternalSession session, PageReference pRef) {
            this.pRef = pRef;
            oldPageListener = pRef.getPageListener();
        }

        @Override
        public PageOperationResult run(InternalScheduler scheduler, boolean waitingIfLocked) {
            if (pRef.isDataStructureChanged()) // 忽略掉
                return PageOperationResult.SUCCEEDED;
            if (!pRef.tryLock(scheduler, waitingIfLocked))
                return PageOperationResult.LOCKED;
            if (pRef.isDataStructureChanged()) { // 需要再判断一次
                pRef.unlock();
                return PageOperationResult.SUCCEEDED;
            }
            if (!pRef.markDirtyPage(oldPageListener)) {
                oldPageListener = pRef.getPageListener();
                pRef.unlock();
                return PageOperationResult.RETRY;
            }
            if (beforeRun()) {
                pRef.unlock();
                return PageOperationResult.SUCCEEDED;
            }
            PageOperationResult res = runLocked(scheduler, waitingIfLocked);
            pRef.unlock();
            return res;
        }

        protected abstract boolean beforeRun(); // 如果返回true可以提前结束

        protected abstract PageOperationResult runLocked(InternalScheduler scheduler,
                boolean waitingIfLocked);

        protected static boolean tryLockParentRef(PageReference pRef, InternalScheduler scheduler,
                boolean waitingIfLocked) {
            PageReference parentRef = pRef.getParentRef();
            if (parentRef.isDataStructureChanged())
                return false;
            if (!parentRef.tryLock(scheduler, waitingIfLocked))
                return false;
            if (parentRef.isDataStructureChanged()) {
                parentRef.unlock();
                return false;
            }
            if (pRef.getParentRef() != parentRef) {
                parentRef.unlock();
                return tryLockParentRef(pRef, scheduler, waitingIfLocked);
            }
            return true;
        }
    }

    // 不处理root leaf page的场景，在Remove类那里已经保证不会删除root leaf page
    private static class RemovePage extends ChildOperation {

        private final Object key;

        public RemovePage(InternalSession session, PageReference pRef, Object key) {
            super(session, pRef);
            this.key = key;
        }

        @Override
        protected boolean beforeRun() {
            Page p = pRef.getPage();
            if (p != null && !p.isEmpty()) {
                // 对父节点加锁失败后会放到异步队列里，并释放pRef的锁
                // 如果后续又增加了新数据，那就直接返回，不能删除了
                return true;
            }
            return false;
        }

        @Override
        protected PageOperationResult runLocked(InternalScheduler scheduler, boolean waitingIfLocked) {
            if (!tryLockParentRef(pRef, scheduler, waitingIfLocked))
                return PageOperationResult.LOCKED;
            PageReference parentRef = pRef.getParentRef();
            Page parent = parentRef.getOrReadPage();
            int index = parent.getPageIndex(key);
            parent = parent.copy();
            parent.remove(index);
            parentRef.replacePage(parent);
            // 先看看父节点是否需要删除
            if (parent.isEmpty()) {
                // 如果是root node page，那么直接替换
                // 新的空页面占用的内存大小不必再计入，原页面已经算在内了
                if (parentRef.isRoot())
                    parent.map.newRoot(parent.map.createEmptyPage(false));
                else
                    asyncRemovePage(scheduler, waitingIfLocked, null, parentRef, key);
            }
            // 非root page被删除后，原有的ref被废弃
            pRef.replacePage(pRef.getPageInfo(), new RemovedPageInfo());
            parentRef.unlock();
            return PageOperationResult.SUCCEEDED;
        }
    }

    private static class SplitPage extends ChildOperation {

        public SplitPage(InternalSession session, PageReference pRef) {
            super(session, pRef);
        }

        @Override
        protected boolean beforeRun() {
            Page p = pRef.getPage();
            if (p != null && !p.needSplit()) {
                // 对父节点加锁失败后会放到异步队列里，并释放pRef的锁
                // 如果后续执行删除操作导致page变小不用切割了，那就直接返回
                return true;
            }
            return false;
        }

        @Override
        protected PageOperationResult runLocked(InternalScheduler scheduler, boolean waitingIfLocked) {
            Page p = pRef.getPage();
            // 如果是root page，那么直接替换，此时的root page可能是leaf page也可能是node page
            if (pRef.isRoot()) {
                TmpNodePage tmpNodePage = splitPage(p);

                BTreeGC bgc = p.map.getBTreeStorage().getBTreeGC();
                bgc.addUsedMemory(-pRef.getPageInfo().getTotalMemory());
                bgc.addUsedMemory(tmpNodePage.parent.getMemory());
                bgc.addUsedMemory(tmpNodePage.left.getPageInfo().getTotalMemory());
                bgc.addUsedMemory(tmpNodePage.right.getPageInfo().getTotalMemory());

                tmpNodePage.parent.setRef(pRef);
                tmpNodePage.left.setParentRef(pRef);
                tmpNodePage.right.setParentRef(pRef);
                if (p.isNode())
                    setParentRef(tmpNodePage);
                pRef.replacePage(tmpNodePage.parent);

                // 放到最后做
                if (tmpNodePage.left.isLeafPage()) {
                    setPageListener(tmpNodePage.left);
                    setPageListener(tmpNodePage.right);
                }
            } else {
                if (!tryLockParentRef(pRef, scheduler, waitingIfLocked))
                    return PageOperationResult.LOCKED;

                TmpNodePage tmpNodePage = splitPage(p); // 先锁再切，避免做无用功
                PageReference parentRef = pRef.getParentRef();
                Page newParent = parentRef.getOrReadPage().copyAndInsertChild(tmpNodePage);
                if (p.isNode())
                    setParentRef(tmpNodePage);
                parentRef.replacePage(newParent);

                // 非root page被切割后，原有的ref被废弃
                // 如果其他事务引用的是一个已经split的节点，让它重定向到父节点
                pRef.replacePage(pRef.getPageInfo(), new SplittedPageInfo(parentRef));

                // 先看看父节点是否需要切割
                if (newParent.needSplit()) {
                    asyncSplitPage(scheduler, waitingIfLocked, null, parentRef);
                }

                // 放到最后做
                if (tmpNodePage.left.isLeafPage()) {
                    setPageListener(tmpNodePage.left);
                    setPageListener(tmpNodePage.right);
                }

                parentRef.unlock();
            }
            return PageOperationResult.SUCCEEDED;
        }

        private static TmpNodePage splitPage(Page p) {
            PageInfo pInfoOld = p.getRef().getPageInfo();
            // 注意: 在这里被切割的页面可能是node page或leaf page
            int at = p.getKeyCount() / 2;
            Object k = p.getSplitKey(at);
            // 切割前必须copy当前被切割的页面，否则其他读线程可能读到切割过程中不一致的数据
            p = p.copy();
            // 对页面进行切割后，会返回右边的新页面，而copy后的当前被切割页面变成左边的新页面
            Page rightChildPage = p.split(at);
            Page leftChildPage = p;
            PageReference leftRef = new PageReference(p.map.getBTreeStorage(), leftChildPage);
            PageReference rightRef = new PageReference(p.map.getBTreeStorage(), rightChildPage);
            PageReference[] children = { leftRef, rightRef };
            Object[] keys = { k };
            Page parent = NodePage.create(p.map, keys, children, 0);
            PageReference parentRef = new PageReference(p.map.getBTreeStorage(), parent);
            parent.setRef(parentRef);
            // 它俩的ParentRef不在这里设置，调用者根据自己的情况设置
            leftChildPage.setRef(leftRef);
            rightChildPage.setRef(rightRef);
            return new TmpNodePage(parent, leftRef, rightRef, k, pInfoOld);
        }

        private static void setPageListener(PageReference ref) {
            PageListener pageListener = ref.getPageListener();
            for (Object obj : ref.getPage().getValues()) {
                if (obj instanceof Lockable)
                    ((Lockable) obj).setPageListener(pageListener);
            }
        }

        private static void setParentRef(TmpNodePage tmpNodePage) {
            PageReference lRef = tmpNodePage.left;
            PageReference rRef = tmpNodePage.right;
            for (PageReference ref : lRef.getPage().getChildren()) {
                ref.setParentRef(lRef);
            }
            for (PageReference ref : rRef.getPage().getChildren()) {
                ref.setParentRef(rRef);
            }
        }
    }

    public static class TmpNodePage {
        final Page parent;
        final PageReference left;
        final PageReference right;
        final Object key;

        public TmpNodePage(Page parent, PageReference left, PageReference right, Object key,
                PageInfo pInfoOld) {
            this.parent = parent;
            this.left = left;
            this.right = right;
            this.key = key;
            parent.getRef().getPageInfo().updateTime(pInfoOld);
            left.getPageInfo().updateTime(pInfoOld);
            right.getPageInfo().updateTime(pInfoOld);
        }
    }
}
