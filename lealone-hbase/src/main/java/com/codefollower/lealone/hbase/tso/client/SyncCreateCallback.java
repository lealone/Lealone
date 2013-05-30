package com.codefollower.lealone.hbase.tso.client;

public class SyncCreateCallback extends SyncCallbackBase implements CreateCallback {
    private long startTimestamp = 0;

    public long getStartTimestamp() {
        return startTimestamp;
    }

    synchronized public void complete(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        countDown();
    }
}
