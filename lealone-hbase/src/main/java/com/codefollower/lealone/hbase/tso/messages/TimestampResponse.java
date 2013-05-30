package com.codefollower.lealone.hbase.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * The message object for sending a commit request to TSO
 * 
 * @author maysam
 * 
 */
public class TimestampResponse implements TSOMessage {

    /**
     * the timestamp
     */
    public long timestamp;

    /**
     * Constructor from timestamp
     * 
     * @param t
     */
    public TimestampResponse(long t) {
        timestamp = t;
    }

    public TimestampResponse() {
    }

    @Override
    public String toString() {
        return "TimestampResponse: T_s:" + timestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) {
        timestamp = aInputStream.readLong();
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(timestamp);
    }
}
