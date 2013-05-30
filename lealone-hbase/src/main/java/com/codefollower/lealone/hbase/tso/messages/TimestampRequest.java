package com.codefollower.lealone.hbase.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

/**
 * The message object for sending a timestamp request to TSO
 * 
 */
public class TimestampRequest implements TSOMessage {

    @Override
    public void readObject(ChannelBuffer aInputStream) {
    }

    @Override
    public void writeObject(DataOutputStream aOutputStream) throws IOException {
    }

}
