package com.yahoo.omid.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.TSOMessage;

/**
 * The message object for sending an abort request to TSO
 * 
 * @author maysam
 * 
 */
public class AbortRequest implements TSOMessage {

    /**
     * Starting timestamp
     */
    public long startTimestamp;

    @Override
    public String toString() {
        return "AbortRequest: T_s:" + startTimestamp;
    }

    @Override
    public void readObject(ChannelBuffer aInputStream) {
        startTimestamp = aInputStream.readLong();
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
        aOutputStream.writeLong(startTimestamp);
    }

}
