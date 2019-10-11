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

import java.util.concurrent.Callable;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.PageKey;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.aose.btree.BTreePage.DynamicInfo;

public abstract class PageOperations {

    public static final boolean ASSERT = false;

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

    // BTree的读操作是不阻塞线程的，所以其实这个类没什么用处
    public static class Get<K, V> implements PageOperation {
        private final BTreeMap<K, V> map;
        private final K key;
        private final AsyncHandler<AsyncResult<V>> handler;
        private BTreePage p;

        public Get(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<V>> handler) {
            this.map = map;
            this.key = key;
            this.handler = handler;
        }

        @Override
        @SuppressWarnings("unchecked")
        public PageOperationResult run(PageOperationHandler currentHandler) {
            if (p == null) {
                p = map.gotoLeafPage(key);
                if (currentHandler != p.getHandler()) {
                    p.addPageOperation(this);
                    return PageOperationResult.SHIFTED;
                }
            }
            p = p.redirectIfSplited(key);
            int index = p.binarySearch(key);
            V result = (V) (index >= 0 ? p.getValue(index, true) : null);
            AsyncResult<V> ar = new AsyncResult<>();
            ar.setResult(result);
            handler.handle(ar);
            return PageOperationResult.SUCCEEDED;
        }
    }

    // 只针对单Key的写操作，包括: Put、PutIfAbsent、Replace、Remove
    public static abstract class SingleWrite<K, V, R> implements PageOperation {
        final BTreeMap<K, V> map;
        final K key;
        final AsyncHandler<AsyncResult<R>> asyncResultHandler;

        // 最终要操作的leaf page
        BTreePage p;

        public SingleWrite(BTreeMap<K, V> map, K key, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
            this.map = map;
            this.key = key;
            this.asyncResultHandler = asyncResultHandler;
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            // 在BTree刚创建时，因为只有一个root leaf page，不适合并行化，
            // 也不适合把所有的写操作都转入root leaf page的处理器队列，
            // 这样会导致root leaf page的处理器队列变得更长，反而不适合并行化了，
            // 所以只有BTree的root page是一个node page，并且子节点数至少大于2时才是并行化的最佳时机。
            if (map.parallelDisabled) {
                synchronized (map) {
                    if (map.parallelDisabled) { // 需要再判断一次，上一个线程会修改这个字段
                        PageOperationResult rageOperationResult = write(currentHandler, false);
                        map.enableParallelIfNeeded();
                        return rageOperationResult;
                    }
                }
            }
            return write(currentHandler, true);
        }

        private PageOperationResult write(PageOperationHandler currentHandler, boolean isShiftEnabled) {
            if (p == null) {
                // 不管当前处理器是不是leaf page的处理器都可以事先定位到leaf page
                p = map.gotoLeafPage(key);

                // 处理分布式场景
                if (p.getLeafPageMovePlan() != null) {
                    writeRemote();
                    return PageOperationResult.SHIFTED;
                }

                // 当前处理器不是leaf page的处理器时需要移交给leaf page的处理器处理
                if (isShiftEnabled && currentHandler != p.getHandler()) {
                    p.addPageOperation(this);
                    return PageOperationResult.SHIFTED;
                }
            }

            // 看看是否被切割了
            p = p.redirectIfSplited(key);

            // 如果已经被删除，重新从root page开始
            DynamicInfo oldDynamicInfo = p.dynamicInfo;
            if (oldDynamicInfo.isRemoved()) {
                p = null;
                return write(currentHandler, true);
            } else if (oldDynamicInfo.isRemoving()) {
                // 如果正在删除中，尝试让它变回正常状态，如果失败了，重新从root page开始
                DynamicInfo newDynamicInfo = new DynamicInfo(BTreePage.State.NORMAL);
                if (!p.updateDynamicInfo(oldDynamicInfo, newDynamicInfo)) {
                    p = null;
                    return write(currentHandler, true);
                }
            }

            if (ASSERT) {
                if (!p.isLeaf() || p.dynamicInfo.state != BTreePage.State.NORMAL
                        || (isShiftEnabled && currentHandler != p.getHandler())) {
                    DbException.throwInternalError();
                }
            }

            int index = p.binarySearch(key);
            Object result = writeLocal(index);
            handleAsyncResult(result); // 可以提前执行回调函数了，不需要考虑后续的代码

            // 看看当前leaf page是否需要进行切割
            // 当index<0时说明是要增加新值，其他操作不切割(暂时不考虑被更新的值过大，导致超过page size的情况)
            if (index < 0 && p.needSplit()) {
                splitLeafPage(p);
                return PageOperationResult.SPLITTING;
            } else {
                return PageOperationResult.SUCCEEDED;
            }
        }

        @SuppressWarnings("unchecked")
        private void handleAsyncResult(Object result) {
            AsyncResult<R> ar = new AsyncResult<>();
            ar.setResult((R) result);
            asyncResultHandler.handle(ar);
        }

        // 这里的index是key所在的leaf page的索引，
        // 可能是新增的key所要插入的index，也可能是将要修改或删除的index
        protected abstract Object writeLocal(int index);

        // 在分布式场景，当前leaf page已经被移到其他节点了
        protected void writeRemote() {
        }

        protected void insertLeaf(int index, V value) {
            index = -index - 1;
            p.insertLeaf(index, key, value);
            map.setMaxKey(key);
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
            Object result;
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            } else {
                result = p.setValue(index, value);
                return result;
            }
        }

        @Override
        protected void writeRemote() {
            map.putRemote(p, key, value, asyncResultHandler);
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreeMap<K, V> map, K key, V value, AsyncHandler<AsyncResult<V>> asyncResultHandler) {
            super(map, key, value, asyncResultHandler);
        }

        @Override
        protected Object writeLocal(int index) {
            if (index < 0) {
                insertLeaf(index, value);
                return null;
            }
            return p.getValue(index);
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
                p.setValue(index, value);
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
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
            Object old = p.getValue(index);
            p.remove(index);
            if (p.isEmpty() && p != p.map.getRootPage()) { // 删除leaf page，但是root leaf page除外
                p.dynamicInfo = new DynamicInfo(BTreePage.State.REMOVING);
                RemoveChild task = new RemoveChild(p, key);
                p.map.pohFactory.getNodePageOperationHandler().handlePageOperation(task);
            }
            return old;
        }

        @Override
        protected void writeRemote() {
            map.removeRemote(p, key, asyncResultHandler);
        }
    }

    private static class PageReferenceContext {
        final BTreePage parent;
        final int index;
        final PageReferenceContext next;

        public PageReferenceContext(BTreePage parent, int index, PageReferenceContext next) {
            this.parent = parent;
            this.index = index;
            this.next = next;
        }
    }

    // 这个类不处理root leaf page被切割的场景，在执行Put操作时已经直接处理，
    // 也就是说此时的btree至少有两层
    public static class AddChild implements PageOperation {
        final TmpNodePage tmpNodePage;

        public AddChild(TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
        }

        private static void splitNodePage(BTreePage p, PageReferenceContext context) {
            if (context == null) { // 说明是root page要切割了
                BTreePage root = splitPage(p).parent;
                p.map.newRoot(root);
            } else {
                // node page的切割直接由单一的node page处理器处理，不会产生并发问题
                int at = p.getKeyCount() / 2;
                Object k = p.getKey(at);
                BTreePage rightChildPage = p.split(at);
                context.parent.setChild(context.index, rightChildPage);
                context.parent.insertNode(context.index, k, p);

                // 如果当前被切割的node page导致它的父节点也需要切割，那么一直继续下去，直到root page
                if (context.parent.needSplit()) {
                    splitNodePage(context.parent, context.next);
                }
            }
        }

        private PageReferenceContext findParentNode(boolean copy) {
            BTreePage root = tmpNodePage.old.map.getRootPage();
            BTreePage p = root;
            if (copy)
                p = p.copy();
            Object key = tmpNodePage.key;
            PageReferenceContext context = null;
            while (p.isNode()) {
                int index = p.binarySearch(key);
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                context = new PageReferenceContext(p, index, context);
                BTreePage c = p.getChildPage(index);
                if (c.isNode()) {
                    c = c.copy();
                    p.setChild(index, c);
                }
                p = c;
            }
            if (context == null) {
                // 此时的btree至少有两层，所以肯定能得到一个context
                throw DbException.throwInternalError("context is null");
            }
            return context;
        }

        @Override
        public void run() {
            PageReferenceContext parentContext = findParentNode(true);
            BTreePage parent = parentContext.parent;

            // 先看看父节点是否需要切割
            if (parent.needSplit()) {
                splitNodePage(parent, parentContext.next);
                parentContext = findParentNode(true);
                parent = parentContext.parent;
            }

            parent.setAndInsertChild(parentContext.index, tmpNodePage);

            parentContext = parentContext.next;
            if (parentContext == null) {
                // 如果下一个context为null，说明当前父节点已经是root node page了，
                // 此时需要替换root node page
                parent.map.newRoot(parent);
            } else {
                parentContext.parent.setChild(parentContext.index, parent);
                parent.map.newRoot(parentContext.parent);
            }
            tmpNodePage.left.page.enableSplit();
            tmpNodePage.right.page.enableSplit();
        }
    }

    // 不处理root leaf page的场景，Remove类那里已经保证不会删除root leaf page
    public static class RemoveChild implements PageOperation {
        final BTreePage old;
        final Object key;

        public RemoveChild(BTreePage old, Object key) {
            this.old = old;
            this.key = key;
        }

        @Override
        public void run() {
            DynamicInfo oldDynamicInfo = old.dynamicInfo;
            // 对于先remove然后put的场景，会快速从Removing状态过度到Normal状态，
            // 可能造成不必要的RemoveChild操作，所以直接忽视RemoveChild操作了
            if (!oldDynamicInfo.isRemoving())
                return;
            BTreePage root = old.map.getRootPage();
            BTreePage p = root.copy();
            remove(p, key);
            if (p.isNode() && p.isEmpty()) {
                p.removePage();
                p = BTreeLeafPage.createEmpty(old.map);
            }
            DynamicInfo newDynamicInfo = new DynamicInfo(BTreePage.State.REMOVED);
            // 状态改变了，可能又有新的数据加到old page中了，那么就放弃这次删除子节点的操作
            if (old.updateDynamicInfo(oldDynamicInfo, newDynamicInfo)) {
                // 虽然先更新old的dynamicInfo字段再更新map的root字段不是原子操作，但依然是安全的，
                // 此时其他线程依然从旧的root page开始找，然后又找到old这个page，
                // 看到它的dynamicInfo字段变成REMOVED了，会继续从root page找，只是多循环了几次，直到这里设置新的root page为止
                old.map.newRoot(p);
            }
        }

        private void remove(BTreePage p, Object key) {
            if (p.isLeaf()) {
                return;
            }
            int index = p.binarySearch(key);
            if (index < 0) {
                index = -index - 1;
            } else {
                index++;
            }
            BTreePage cOld = p.getChildPage(index);
            BTreePage c = cOld.copy();
            remove(c, key);
            if (c.isNotEmpty()) {
                // no change, or there are more nodes
                p.setChild(index, c);
            } else {
                PageKey pageKey = p.getChildPageReference(index).pageKey;
                // this child was deleted
                if (p.getKeyCount() == 0) { // 如果p的子节点只剩一个叶子节点时，keyCount为0
                    p.setChild(index, c);
                    c.removePage(); // 直接删除最后一个子节点，父节点在remove(Object)那里删除
                } else {
                    p.remove(index); // 删除没有记录的子节点
                }
                if (c.isLeaf())
                    old.map.fireLeafPageRemove(pageKey, c);
            }
        }
    }

    public static class TmpNodePage {
        final BTreePage parent;
        final BTreePage old;
        final PageReference left;
        final PageReference right;
        final Object key;

        public TmpNodePage(BTreePage parent, BTreePage old, PageReference left, PageReference right, Object key) {
            this.parent = parent;
            this.old = old;
            this.left = left;
            this.right = right;
            this.key = key;
        }
    }

    private static void splitLeafPage(BTreePage p) {
        // 第一步:
        // 切开page，得到一个临时的父节点和两个新的leaf page
        // 临时父节点只能通过被切割的page重定向访问
        TmpNodePage tmp = splitPage(p);

        // 第二步:
        // 如果是对root leaf page进行切割，因为当前只有一个线程在处理，所以直接替换root即可，这是安全的
        if (p == p.map.getRootPage()) {
            p.map.newRoot(tmp.parent);
            return;
        }

        // 第三步:
        // 禁用新leaf page的切割功能，
        // 避免在父节点完成AddChild操作前又产生出新的page，这会引入不必要的复杂性
        tmp.left.page.disableSplit();
        tmp.right.page.disableSplit();

        // 第四步:
        // 先重定向到临时的父节点，等实际的父节点完成AddChild操作后再修正
        BTreePage.DynamicInfo dynamicInfo = new BTreePage.DynamicInfo(BTreePage.State.SPLITTED, tmp.parent);
        p.dynamicInfo = dynamicInfo;

        // 第五步:
        // 把AddChild操作放入父节点的处理器队列中，等候处理。
        // leaf page的切割需要更新父节点的相关数据，所以交由父节点处理器处理，避免引入复杂的并发问题
        AddChild task = new AddChild(tmp);
        p.map.pohFactory.getNodePageOperationHandler().handlePageOperation(task);

        // 第六步:
        // 对于分布式场景，通知发生切割了，需要选一个leaf page来移动
        p.map.fireLeafPageSplit(tmp.key);
    }

    private static TmpNodePage splitPage(BTreePage p) {
        // 注意: 在这里被切割的页面可能是node page或leaf page
        int at = p.getKeyCount() / 2;
        Object k = p.getKey(at);
        // 切割前必须copy当前被切割的页面，否则其他读线程可能读到切割过程中不一致的数据
        BTreePage old = p;
        p = p.copy();
        // 对页面进行切割后，会返回右边的新页面，而copy后的当前被切割页面变成左边的新页面
        BTreePage rightChildPage = p.split(at);
        BTreePage leftChildPage = p;
        PageReference leftRef = new PageReference(leftChildPage, k, true);
        PageReference rightRef = new PageReference(rightChildPage, k, false);
        Object[] keys = { k };
        PageReference[] children = { leftRef, rightRef };
        BTreePage parent = BTreePage.createNode(p.map, keys, children, 0);
        return new TmpNodePage(parent, old, leftRef, rightRef, k);
    }
}
