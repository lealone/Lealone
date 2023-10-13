/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import java.util.concurrent.CountDownLatch;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.Session;

public interface PageOperation {

    public static enum PageOperationResult {
        SUCCEEDED,
        RETRY,
        LOCKED;
    }

    default PageOperationResult run(PageOperationHandler currentHandler) {
        return run(currentHandler, true);
    }

    default PageOperationResult run(PageOperationHandler currentHandler, boolean waitingIfLocked) {
        return PageOperationResult.SUCCEEDED;
    }

    default Session getSession() {
        return null;
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
