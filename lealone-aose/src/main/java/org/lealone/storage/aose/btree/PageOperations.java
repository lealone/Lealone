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
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;

public abstract class PageOperations {

    private PageOperations() {
    }

    public static class Get<K, V> implements PageOperation {
        BTreePage p;
        K key;
        AsyncHandler<AsyncResult<V>> handler;

        public Get(BTreePage p, K key, AsyncHandler<AsyncResult<V>> handler) {
            this.p = p;
            this.key = key;
            this.handler = handler;
        }

        @Override
        public void run() {
            while (true) {
                int index = p.binarySearch(key);
                if (p.isLeaf()) {
                    @SuppressWarnings("unchecked")
                    V result = (V) (index >= 0 ? p.getValue(index, true) : null);
                    AsyncResult<V> ar = new AsyncResult<>();
                    ar.setResult(result);
                    handler.handle(ar);
                    break;
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

    public static class CallableOperation implements PageOperation {
        final Callable<?> callable;

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

    public static class WriteOperation implements PageOperation {
        final Runnable runnable;

        public WriteOperation(Runnable task) {
            runnable = task;
        }

        @Override
        public void run() {
            runnable.run();
        }
    }

    public static class Put<K, V, R> implements PageOperation {

        BTreePage root;
        BTreePage p;
        final K key;
        final V value;
        final AsyncHandler<AsyncResult<R>> asyncResultHandler;
        boolean searched;

        public Put(BTreePage p, K key, V value, AsyncHandler<AsyncResult<R>> asyncResultHandler) {
            this.root = p.map.root;
            this.p = p;
            this.key = key;
            this.value = value;
            this.asyncResultHandler = asyncResultHandler;
        }

        private void binarySearchLeafPage() {
            root = p.map.getRootPage();
            p = root;
            while (p.isNode()) {
                int index = p.binarySearch(key);
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                p = p.getChildPage(index);
            }
            searched = true;
        }

        // private boolean binarySearchLeafPage(PageOperationHandler currentHandler) {
        // root = p.map.getRootPage();
        // p = root;
        // while (p.isNode()) {
        // int index = p.binarySearch(key);
        // if (index < 0) {
        // index = -index - 1;
        // } else {
        // index++;
        // }
        // p = p.getChildPage(index);
        // }
        //
        // searched = true;
        // if (currentHandler != p.getHandler()) {
        // p.addTask(this);
        // return false;
        // } else {
        // return true;
        // }
        // }

        private static void splitLeafPage(BTreePage p) {
            BTreeMap.splitCount.incrementAndGet();
            // 第一步:
            // 切开page，得到一个临时的父节点和两个新的leaf page
            // 临时父节点只能通过被切割的page重定向访问
            TmpNodePage tmp = splitPage(p);

            // 第二步:
            // 把临时父节点和两个leaf page的处理器重置为原先page的处理器，并且禁用新leaf page的切割功能，
            // 避免在父节点完成AddChild操作前又产生出新的page，这会引入不必要的复杂性
            tmp.parent.handler = p.handler;
            tmp.left.page.handler = p.handler;
            tmp.right.page.handler = p.handler;
            tmp.left.page.disableSplit();
            tmp.right.page.disableSplit();

            // 第三步:
            // 先重定向到临时的父节点，等实际的父节点完成AddChild操作后再修正
            BTreePage.DynamicInfo dynamicInfo = new BTreePage.DynamicInfo(BTreePage.State.SPLITTED, tmp.parent);
            p.dynamicInfo = dynamicInfo;

            // 第四步:
            // 把AddChild操作放入父节点的处理器队列中，等候处理。
            // leaf page的切割需要更新父节点的相关数据，所以交由父节点处理器处理，避免引入复杂的并发问题
            AddChild task = new AddChild(tmp);
            PageOperationHandlerFactory.getNodePageOperationHandler().handlePageOperation(task);
        }

        @SuppressWarnings("unchecked")
        private void handleAsyncResult(Object result) {
            AsyncResult<R> ar = new AsyncResult<>();
            ar.setResult((R) result);
            asyncResultHandler.handle(ar);
        }

        protected Object putSync() {
            return p.map.put(key, value, p);
        }

        @Override
        public PageOperationResult run(PageOperationHandler currentHandler) {
            // 在BTree刚创建时，因为只有一个root page，不适合并行化，
            // 也不适合把所有的put操作都转入root page的处理器队列，
            // 这样会导致root page的处理器队列变得更长，反而不适合并行化了，
            // 所以只有BTree的leaf page数大于等于线程数时才是并行化的最佳时机。
            if (p.map.disableParallel) {
                // 当进入这个if分支准备进行put时，可能其他线程已经完成并行化阶段前的写入了，所以put会返回REDIRECT
                Object result = putSync();
                if (result != BTreeMap.REDIRECT) {
                    handleAsyncResult(result);
                    return PageOperationResult.SUCCEEDED;
                }
            }
            if (!searched) {
                binarySearchLeafPage();

                // 当前处理器不是leaf page的处理器时需要移交给leaf page的处理器处理
                if (currentHandler != p.getHandler()) {
                    p.addTask(this);
                    return PageOperationResult.SHIFTED;
                }
            }

            // if (p.dynamicInfo.state == BTreePage.State.SPLITTED) {
            // if (root != p.map.root) {
            // binarySearchLeafPage();
            // if (currentHandler != p.getHandler()) {
            // new Error("currentHandler: " + currentHandler + ", leafPageHandler: " + p.getHandler())
            // .printStackTrace();
            // System.exit(-1);
            // }
            //
            // if (p.dynamicInfo.state == BTreePage.State.SPLITTED) {
            // // new Error("dynamicInfo: " + p.dynamicInfo.state).printStackTrace();
            // // System.exit(-1);
            //
            // p = p.dynamicInfo.redirect;
            // int index;
            // if (p.map.getKeyType().compare(key, p.getKey(0)) < 0)
            // index = 0;
            // else
            // index = 1;
            // p = p.getChildPage(index);
            // }
            // } else {
            // p = p.dynamicInfo.redirect;
            // int index;
            // if (p.map.getKeyType().compare(key, p.getKey(0)) < 0)
            // index = 0;
            // else
            // index = 1;
            // p = p.getChildPage(index);
            // }
            // }

            int count = 0;
            while (p.dynamicInfo.state == BTreePage.State.SPLITTED) {
                // leaf page处在切割状态时，重定向到临时的node page了
                p = p.dynamicInfo.redirect;
                int index;
                if (p.map.getKeyType().compare(key, p.getKey(0)) < 0)
                    index = 0;
                else
                    index = 1;
                p = p.getChildPage(index);
                count++;
            }
            if (count > 7) {
                // System.out.println("redirect count: " + count);
            }

            // if (p.isNode() || root != p.map.root) {
            // if (!binarySearchLeafPage(currentHandler)) {
            // return PageOperationResult.SUCCEEDED;
            // }
            // }
            //
            // String str = "";
            // switch (p.dynamicInfo.state) {
            // case SPLITTING:
            // BTreePage redirect = p.dynamicInfo.redirect;
            // int index;
            // if (p.map.getKeyType().compare(key, redirect.getKey(0)) < 0)
            // index = 0;
            // else
            // index = 1;
            // p = redirect.getChildPage(index);
            // str = "aaa";
            // while (p.dynamicInfo.state == BTreePage.State.SPLITTING) {
            // // leaf page处在切割状态时，重定向到临时的node page了
            // redirect = p.dynamicInfo.redirect;
            // if (p.map.getKeyType().compare(key, redirect.getKey(0)) < 0)
            // index = 0;
            // else
            // index = 1;
            // p = redirect.getChildPage(index);
            // str += "aaa";
            //
            // new Error(str + " " + p.dynamicInfo.state).printStackTrace();
            // }
            // break;
            // case SPLITTED:
            // if (!binarySearchLeafPage(currentHandler)) {
            // return PageOperationResult.SUCCEEDED;
            // }
            // str = "bbb";
            // }

            int index = p.binarySearch(key);
            Object result = put(index);
            handleAsyncResult(result);

            PageOperationResult rageOperationResult;
            if (!p.map.disableSplit && p.isSplitEnabled() && p.needSplit()) {
                splitLeafPage(p);
                rageOperationResult = PageOperationResult.SPLITTING;
            } else {
                rageOperationResult = PageOperationResult.SUCCEEDED;
            }
            return rageOperationResult;
        }

        protected Object put(int index) {
            Object result;
            if (index < 0) {
                if (p.dynamicInfo.state != BTreePage.State.NORMAL) {
                    String str = "aaa";
                    new Error(str + " " + p.dynamicInfo.state).printStackTrace();
                    System.exit(-1);
                }
                index = -index - 1;
                p.insertLeaf(index, key, value);
                p.map.setMaxKey(key);
                BTreeMap.addCount.incrementAndGet();
                // 新增数据时才需要更新父节点的计数器
                PageOperationHandlerFactory.getNodePageOperationHandler()
                        .handlePageOperation(new UpdateParentCounter(p, key, true));
                result = null;

                // if (!p.isSplitEnabled() && p.getCounter().get() > 20) {
                // System.out.println("leaf page keys: " + p.getCounter().get());
                // }
            } else {
                result = p.setValue(index, value);
                BTreeMap.putCount.incrementAndGet();
            }
            return result;
        }
    }

    public static class PutIfAbsent<K, V> extends Put<K, V, V> {

        public PutIfAbsent(BTreePage p, K key, V value, AsyncHandler<AsyncResult<V>> asyncResultHandler) {
            super(p, key, value, asyncResultHandler);
        }

        @Override
        protected Object putSync() {
            return p.map.putIfAbsent(key, value, p);
        }

        @Override
        protected Object put(int index) {
            if (index < 0) {
                return super.put(index);
            }
            return p.getValue(index);
        }
    }

    public static class Replace<K, V> extends Put<K, V, Boolean> {
        final V oldValue;

        public Replace(BTreePage p, K key, V oldValue, V newValue,
                AsyncHandler<AsyncResult<Boolean>> asyncResultHandler) {
            super(p, key, newValue, asyncResultHandler);
            this.oldValue = oldValue;
        }

        @Override
        protected Boolean put(int index) {
            Object old = p.getValue(index);
            if (index < 0 || p.map.areValuesEqual(old, oldValue)) {
                super.put(index);
                return Boolean.TRUE;
            }
            return Boolean.FALSE;
        }
    }

    public static class Remove<K, V> extends Put<K, V, V> {

        public Remove(BTreePage p, K key, AsyncHandler<AsyncResult<V>> asyncResultHandler) {
            super(p, key, null, asyncResultHandler);
        }

        @Override
        protected Object put(int index) {
            if (index < 0) {
                return null;
            }
            Object old = p.getValue(index);
            p.remove(index);
            return old;
        }
    }

    public static class UpdateParentCounter implements PageOperation {
        final BTreePage p;
        final Object key;
        final boolean increment;

        public UpdateParentCounter(BTreePage p, Object key, boolean increment) {
            BTreeMap.addUpdateCounterTaskCount.incrementAndGet();
            this.p = p;
            this.key = key;
            this.increment = increment;
        }

        @Override
        public void run() {
            BTreeMap.runUpdateCounterTaskCount.incrementAndGet();
            BTreePage p = this.p.map.getRootPage();
            while (p.isNode()) {
                // System.out.println(p.getCounter().get());
                if (increment)
                    p.getCounter().getAndIncrement();
                else
                    p.getCounter().getAndDecrement();
                int index = p.binarySearch(key);
                if (index < 0) {
                    index = -index - 1;
                } else {
                    index++;
                }
                p = p.getChildPage(index);
            }
        }
    }

    public static class AddChild implements PageOperation {
        final TmpNodePage tmpNodePage;

        public AddChild(TmpNodePage tmpNodePage) {
            this.tmpNodePage = tmpNodePage;
        }

        private static void splitNodePage(BTreePage p, PageReferenceContext context) {
            BTreeMap.splitCount.incrementAndGet();
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
            BTreePage p = tmpNodePage.old.map.getRootPage();
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
                // 因为前提已经保证只有在root page为node page时才运行并行化操作了，所以肯定能得到一个context
                throw DbException.throwInternalError("context is null");
            }
            return context;
        }

        @Override
        public void run() {
            // long t1 = System.currentTimeMillis();
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
            // BTreePage.DynamicInfo dynamicInfo = new BTreePage.DynamicInfo(BTreePage.State.SPLITTED, parent);
            // tmpNodePage.old.dynamicInfo = dynamicInfo;
            tmpNodePage.left.page.enableSplit();
            tmpNodePage.right.page.enableSplit();
            // long t2 = System.currentTimeMillis();
            // System.out.println("add child time: " + (t2 - t1) + " ms");
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

    private static TmpNodePage splitPage(BTreePage p) {
        // 注意: 在这里被切割的页面可能是node page或leaf page
        // 如果是leaf page，那么totalCount和getKeyCount是一样的，
        // 如果是node page，那么totalCount和getKeyCount是不一样的，
        // 此时totalCount是归属于当前node page的所有leaf page中的记录总数，
        // 而getKeyCount只是当前node page中包含的key的个数，也就是它的直接child个数减一。
        long totalCount = p.getTotalCount();
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
        BTreePage parent = BTreePage.create(p.map, keys, null, children, new AtomicLong(totalCount), 0);
        return new TmpNodePage(parent, old, leftRef, rightRef, k);
    }
}
