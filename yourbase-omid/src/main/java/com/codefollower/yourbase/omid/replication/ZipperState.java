package com.codefollower.yourbase.omid.replication;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.codefollower.yourbase.omid.tso.TSOMessage;

public class ZipperState implements TSOMessage {

    private long lastStartTimestamp;
    private long lastCommitTimestamp;

    private long lastHalfAbortedTimestamp;
    private long lastFullAbortedTimestamp;

    public ZipperState() {
    }

    public ZipperState(long lastStartTimestamp, long lastCommitTimestamp, long lastHalfAbortedTimestamp,
            long lastFullAbortedTimestamp) {
        this.lastStartTimestamp = lastStartTimestamp;
        this.lastCommitTimestamp = lastCommitTimestamp;
        this.lastHalfAbortedTimestamp = lastHalfAbortedTimestamp;
        this.lastFullAbortedTimestamp = lastFullAbortedTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer buffer) {
        lastStartTimestamp = buffer.readLong();
        lastCommitTimestamp = buffer.readLong();

        lastHalfAbortedTimestamp = buffer.readLong();
        lastFullAbortedTimestamp = buffer.readLong();
    }

    @Override
    public void writeObject(DataOutputStream buffer) throws IOException {
        buffer.writeLong(lastStartTimestamp);
        buffer.writeLong(lastCommitTimestamp);
        buffer.writeLong(lastHalfAbortedTimestamp);
        buffer.writeLong(lastFullAbortedTimestamp);
    }

    public long getLastStartTimestamp() {
        return lastStartTimestamp;
    }

    public void setLastStartTimestamp(long lastStartTimestamp) {
        this.lastStartTimestamp = lastStartTimestamp;
    }

    public long getLastCommitTimestamp() {
        return lastCommitTimestamp;
    }

    public void setLastCommitTimestamp(long lastCommitTimestamp) {
        this.lastCommitTimestamp = lastCommitTimestamp;
    }

    public long getLastHalfAbortedTimestamp() {
        return lastHalfAbortedTimestamp;
    }

    public void setLastHalfAbortedTimestamp(long lastHalfAbortedTimestamp) {
        this.lastHalfAbortedTimestamp = lastHalfAbortedTimestamp;
    }

    public long getLastFullAbortedTimestamp() {
        return lastFullAbortedTimestamp;
    }

    public void setLastFullAbortedTimestamp(long lastFullAbortedTimestamp) {
        this.lastFullAbortedTimestamp = lastFullAbortedTimestamp;
    }

}
