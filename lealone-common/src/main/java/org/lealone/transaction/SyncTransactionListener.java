/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;

public class SyncTransactionListener implements TransactionListener {

    private final CountDownLatch latch = new CountDownLatch(1);
    private volatile RuntimeException e;

    @Override
    public void operationUndo() {
        latch.countDown();
    }

    @Override
    public void operationComplete() {
        latch.countDown();
    }

    @Override
    public void setException(RuntimeException e) {
        this.e = e;
    }

    @Override
    public RuntimeException getException() {
        return e;
    }

    @Override
    public void await() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            setException(DbException.convert(e));
        }
        if (e != null)
            throw e;
    }
}
