/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.concurrent.CountDownLatch;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.async.AsyncTask;

public interface PageOperation extends AsyncTask {

    public static enum PageOperationResult {
        SPLITTING,
        SUCCEEDED,
        SHIFTED,
        REMOTE_WRITTING,
        RETRY,
        FAILED,
        LOCKED;
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

        default void startListen() {
        }

        V await();
    }

    interface ListenerFactory<V> {
        Listener<V> createListener();
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
