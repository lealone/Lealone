package com.codefollower.lealone.hbase.tso.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.codefollower.lealone.hbase.tso.messages.TSOMessage;
import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;

public class TSOEncoder extends OneToOneEncoder {

    //just override decode method
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        ByteArrayOutputStream buffer = BufferPool.getBuffer();
        buffer.reset();
        DataOutputStream objWrapper = new DataOutputStream(buffer);
        if (msg instanceof ChannelBuffer) {
            return msg;
        } else if (msg instanceof TimestampRequest) {
            objWrapper.writeByte(TSOMessage.TimestampRequest);
        } else if (msg instanceof TimestampResponse) {
            objWrapper.writeByte(TSOMessage.TimestampResponse);
        } else
            throw new Exception("Wrong obj");
        ((TSOMessage) msg).writeObject(objWrapper);
        ChannelBuffer result = ChannelBuffers.wrappedBuffer(buffer.toByteArray());
        BufferPool.pushBuffer(buffer);
        return result;
    }
}
