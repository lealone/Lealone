/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.perf.btree;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.storage.DefaultPageOperationHandler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.aose.btree.BTreeMap;
import org.lealone.storage.aose.btree.BTreePage;
import org.lealone.storage.aose.btree.PageReference;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class AsyncBTreePerfTest extends StorageMapPerfTestBase {

    public static void main(String[] args) throws Exception {
        new AsyncBTreePerfTest().run();
    }

    private BTreeMap<Integer, String> btreeMap;
    private DefaultPageOperationHandler[] handlers;

    @Override
    protected void testWrite(int loop) {
        multiThreadsRandomWriteAsync(loop);
        multiThreadsSerialWriteAsync(loop);
    }

    @Override
    protected void testRead(int loop) {
        multiThreadsRandomRead(loop);
        multiThreadsSerialRead(loop);

        // multiThreadsRandomReadAsync(loop);
        // multiThreadsSerialReadAsync(loop);
    }

    @Override
    protected void testConflict(int loop) {
        testConflict(loop, true);
    }

    @Override
    protected void beforeRun() {
        createPageOperationHandlers();
        super.beforeRun();
        // printLeafPageOperationHandlerPercent();
        // printShiftCount(conflictKeys);
    }

    private void createPageOperationHandlers() {
        handlers = new DefaultPageOperationHandler[threadCount];
        for (int i = 0; i < threadCount; i++) {
            handlers[i] = new DefaultPageOperationHandler(i, config);
        }
        PageOperationHandlerFactory f = storage.getPageOperationHandlerFactory();
        f.setLeafPageOperationHandlers(handlers);
    }

    void printShiftCount(int[] keys) {
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        for (int key : keys) {
            BTreePage p = btreeMap.gotoLeafPage(key);
            PageOperationHandler handler = p.getHandler();
            Integer count = map.get(handler);
            if (count == null)
                count = 1;
            else
                count++;
            map.put(handler, count);
        }

        System.out.println("key count: " + keys.length);
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / keys.length * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    void printLeafPageOperationHandlerPercent() {
        BTreePage root = btreeMap.getRootPage();
        HashMap<PageOperationHandler, Integer> map = new HashMap<>();
        AtomicLong leafPageCount = new AtomicLong(0);
        if (root.isLeaf()) {
            map.put(root.getHandler(), 1);
            leafPageCount.incrementAndGet();
        } else {
            findLeafPage(root, map, leafPageCount);
        }
        System.out.println("leaf page count: " + leafPageCount.get());
        System.out.println("handler factory: " + storage.getPageOperationHandlerFactory().getClass().getSimpleName());
        for (HashMap.Entry<PageOperationHandler, Integer> e : map.entrySet()) {
            String percent = String.format("%#.2f", (e.getValue() * 1.0 / leafPageCount.get() * 100));
            System.out.println(e.getKey() + " percent: " + percent + "%");
        }
        System.out.println();
    }

    private void findLeafPage(BTreePage p, HashMap<PageOperationHandler, Integer> map, AtomicLong leafPageCount) {
        if (p.isNode()) {
            for (PageReference ref : p.getChildren()) {
                BTreePage child = ref.getPage();
                if (child.isLeaf()) {
                    PageOperationHandler handler = child.getHandler();
                    // System.out.println("handler: " + handler);
                    Integer count = map.get(handler);
                    if (count == null)
                        count = 1;
                    else
                        count++;
                    map.put(handler, count);
                    leafPageCount.incrementAndGet();
                } else {
                    findLeafPage(child, map, leafPageCount);
                }
            }
        }
    }

    @Override
    protected void init() {
        super.init();
    }

    @Override
    protected void openMap() {
        if (map == null || map.isClosed()) {
            map = btreeMap = storage.openBTreeMap(AsyncBTreePerfTest.class.getSimpleName(), ValueInt.type,
                    ValueString.type, null);
        }
    }

    @Override
    protected Thread getThread(PageOperation pageOperation, int index, int start) {
        DefaultPageOperationHandler h = handlers[index];
        h.reset(false);
        h.handlePageOperation(pageOperation);
        return new Thread(h, h.getName());
    }

    @Override
    public DefaultPageOperationHandler getDefaultPageOperationHandler(int index) {
        return handlers[index];
    }
}
