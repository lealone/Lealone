package com.codefollower.lealone.hbase.tso.client;

import java.util.concurrent.CountDownLatch;

class SyncCallbackBase implements Callback {
    private Exception e = null;
    private CountDownLatch latch = new CountDownLatch(1);

    public Exception getException() {
        return e;
    }

    synchronized public void error(Exception e) {
        this.e = e;
        countDown();
    }

    protected void countDown() {
        latch.countDown();
    }

    public void await() throws InterruptedException {
        latch.await();
    }
}