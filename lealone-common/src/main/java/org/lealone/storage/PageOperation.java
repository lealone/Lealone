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
package org.lealone.storage;

import java.util.concurrent.CountDownLatch;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.AsyncTask;

public interface PageOperation extends AsyncTask {

    public static enum PageOperationResult {
        SPLITTING,
        SUCCEEDED,
        SHIFTED,
        FAILED,
        LOCKED;
    }

    public static enum PageOperationType {
        Get,
        Put,
        PutIfAbsent,
        Replace,
        Remove,
        AddChild,
        Other;
    }

    default PageOperationType getType() {
        return PageOperationType.Other;
    }

    @Override
    default int getPriority() {
        return MAX_PRIORITY;
    }

    @Override
    default void run() {
        // Thread t = Thread.currentThread();
        // if (t instanceof PageOperationHandler) {
        // run((PageOperationHandler) t);
        // } else {
        // run(null);
        // }
    }

    default PageOperationResult run(PageOperationHandler currentHandler) {
        run();
        return PageOperationResult.SUCCEEDED;
    }

    interface Listener<V> extends AsyncHandler<AsyncResult<V>> {
        V await();
    }

    class SyncListener<V> implements Listener<V> {

        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile RuntimeException e;
        private volatile V result;

        @Override
        public V await() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                this.e = new RuntimeException(e);
            }
            if (e != null)
                throw e;
            return result;
        }

        @Override
        public void handle(AsyncResult<V> ar) {
            if (ar.isSucceeded())
                result = ar.getResult();
            else
                e = new RuntimeException(ar.getCause());
            latch.countDown();
        }
    }
}
