/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

public class AsyncResult<T> {

    protected T result;
    protected Throwable cause;
    protected boolean succeeded;
    protected boolean failed;

    public AsyncResult() {
    }

    public AsyncResult(T result) {
        setResult(result);
    }

    public AsyncResult(Throwable cause) {
        setCause(cause);
    }

    public T getResult() {
        return result;
    }

    public void setResult(T result) {
        this.result = result;
        failed = false;
        succeeded = true;
    }

    public Throwable getCause() {
        return cause;
    }

    public void setCause(Throwable cause) {
        this.cause = cause;
        failed = true;
        succeeded = false;
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public void setSucceeded(boolean succeeded) {
        this.succeeded = succeeded;
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }
}
