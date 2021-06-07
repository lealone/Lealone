/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import java.util.ArrayList;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

abstract class ReplicationHandler<T> implements AsyncHandler<AsyncResult<T>> {

    protected final AsyncHandler<AsyncResult<T>> finalResultHandler;
    protected final ArrayList<AsyncResult<T>> results = new ArrayList<>();
    private final ArrayList<Throwable> exceptions = new ArrayList<>();
    private final int ackNodes; // 需要收到响应的节点数
    private int ackCount; // 已收到响应数

    public ReplicationHandler(int ackNodes, AsyncHandler<AsyncResult<T>> finalResultHandler) {
        this.ackNodes = ackNodes;
        this.finalResultHandler = finalResultHandler;
    }

    abstract void onSuccess();

    @Override
    public synchronized void handle(AsyncResult<T> ar) {
        ackCount++;
        if (ar.isSucceeded()) {
            handleResult(ar);
        } else {
            handleException(ar.getCause());
        }
    }

    private void handleResult(AsyncResult<T> result) {
        results.add(result);
        if (ackCount >= ackNodes) {
            onSuccess();
        }
    }

    private void handleException(Throwable t) {
        int errorCode = DbException.convert(t).getErrorCode();
        if (errorCode == ErrorCode.CONNECTION_BROKEN_1 || errorCode == ErrorCode.NETWORK_TIMEOUT_1) {
            // TODO
        }
        exceptions.add(t);
        if (ackCount >= ackNodes && finalResultHandler != null) {
            AsyncResult<T> ar = new AsyncResult<>();
            ar.setCause(exceptions.get(0));
            finalResultHandler.handle(ar);
        }
    }
}
