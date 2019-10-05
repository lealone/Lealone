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

import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueString;
import org.lealone.storage.DefaultPageOperationHandler;
import org.lealone.storage.PageOperation;
import org.lealone.storage.PageOperationHandler;
import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.aose.btree.BTreeMap;

// -Xms512M -Xmx512M -XX:+PrintGCDetails -XX:+PrintGCTimeStamps
public class AsyncBTreePerfTest extends StorageMapPerfTestBase {

    public static void main(String[] args) throws Exception {
        new AsyncBTreePerfTest().run();
    }

    private BTreeMap<Integer, String> btreeMap;
    private DefaultPageOperationHandler[] handlers;

    @Override
    protected void testWrite(int loop) {
        btreeMap.disableParallel = false;
        btreeMap.disableSplit = true;

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
    protected void init() {
        super.init();
        PageOperationHandlerFactory f = storage.getPageOperationHandlerFactory();
        PageOperationHandler[] leafPageOperationHandlers = f.getLeafPageOperationHandlers();
        int handlerCount = leafPageOperationHandlers.length;
        handlers = new DefaultPageOperationHandler[handlerCount];
        for (int i = 0; i < handlerCount; i++) {
            handlers[i] = (DefaultPageOperationHandler) leafPageOperationHandlers[i];
        }
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
        h.reset();
        h.handlePageOperation(pageOperation);
        return new Thread(h, h.getName());
    }
}
