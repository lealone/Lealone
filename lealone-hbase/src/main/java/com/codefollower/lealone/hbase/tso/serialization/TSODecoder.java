package com.codefollower.lealone.hbase.tso.serialization;

import java.io.EOFException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.codefollower.lealone.hbase.tso.messages.TSOMessage;
import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;

public class TSODecoder extends FrameDecoder {

    public TSODecoder() {
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Mark the current buffer position before any reading
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        TSOMessage msg;
        try {
            byte type = buf.readByte();
            switch (type) {
            case TSOMessage.TimestampRequest:
                msg = new TimestampRequest();
                break;
            case TSOMessage.TimestampResponse:
                msg = new TimestampResponse();
                break;
            default:
                throw new Exception("Wrong type " + type + " (" + Integer.toHexString(type) + ") " + buf.toString().length());
            }
            msg.readObject(buf);
        } catch (IndexOutOfBoundsException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        } catch (EOFException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        }

        return msg;
    }
}
