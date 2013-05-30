package com.codefollower.lealone.hbase.tso.server;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;

/**
 * ChannelHandler for the TSO Server
 * 一个TSO Server只对应一个TSOHandler实例
 *
 */
public class TSOHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(TSOHandler.class);

    /**
     * Channel Group
     */
    private final ChannelGroup channelGroup;

    /**
     * Timestamp Oracle
     */
    private final TimestampOracle timestampOracle;

    /**
     * Constructor
     * @param channelGroup
     */
    public TSOHandler(ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        this.timestampOracle = new TimestampOracle();
    }

    public void start() {
    }

    public void stop() {
    }

    /**
     * If write of a message was not possible before, we can do it here
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        Channels.close(e.getChannel());
    }

    /**
     * Handle receieved messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof TimestampRequest) {
            handle((TimestampRequest) msg, ctx);
        }
    }

    /**
     * Handle the TimestampRequest message
     */
    private void handle(TimestampRequest msg, ChannelHandlerContext ctx) {
        long timestamp;
        synchronized (timestampOracle) {
            try {
                timestamp = timestampOracle.next();
            } catch (IOException e) {
                LOG.error("failed to return the next timestamp", e);
                return;
            }
        }

        Channel channel = ctx.getChannel();
        Channels.write(channel, new TimestampResponse(timestamp));
    }

}
