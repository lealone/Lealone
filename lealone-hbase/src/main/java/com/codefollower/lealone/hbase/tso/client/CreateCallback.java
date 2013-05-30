package com.codefollower.lealone.hbase.tso.client;

public interface CreateCallback extends Callback {
    public void complete(long startTimestamp);
}
