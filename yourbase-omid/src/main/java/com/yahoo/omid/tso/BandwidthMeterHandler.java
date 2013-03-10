/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

public class BandwidthMeterHandler extends SimpleChannelHandler {

    private AtomicLong bytesSent = new AtomicLong();
    private AtomicLong bytesReceived = new AtomicLong();
    private double bytesSentPerSecond;
    private double bytesReceivedPerSecond;

    private CollectBandwidthStats collectStats;

    /**
    * Collect a 60-second, moving average of the bytes sent/received per second
    */
    private class CollectBandwidthStats {
        private long lastTimestamp = 0;
        private long lastBytesSent = bytesSent.get();
        private long lastBytesReceived = bytesReceived.get();

        public void run() {

            final long timestamp = System.currentTimeMillis();
            final long deltaTime = timestamp - lastTimestamp;
            long sent = bytesSent.get() - lastBytesSent;
            long received = bytesReceived.get() - lastBytesReceived;
            lastBytesSent = bytesSent.get();
            lastBytesReceived = bytesReceived.get();

            if (sent < 0)
                sent = 0;

            if (received < 0)
                received = 0;

            bytesSentPerSecond = (sent / (double) deltaTime) * 1000;
            bytesReceivedPerSecond = (received / (double) deltaTime) * 1000;

            lastTimestamp = timestamp;
        }
    }

    /**
    * Constructs a new instance without time based statistics.
    * {@link #getBytesSentPerSecond()} and {@link #getBytesReceivedPerSecond()} will not work.
    * For these statistics, instantiate with {@link #BandwidthMeterHandler(org.jboss.netty.util.Timer)} instead.
    */
    public BandwidthMeterHandler() {
        collectStats = new CollectBandwidthStats();
    }

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof MessageEvent && ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
            ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
            bytesReceived.addAndGet(b.readableBytes());
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void handleDownstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof MessageEvent && ((MessageEvent) e).getMessage() instanceof ChannelBuffer) {
            ChannelBuffer b = (ChannelBuffer) ((MessageEvent) e).getMessage();
            bytesSent.addAndGet(b.readableBytes());
        }
        super.handleDownstream(ctx, e);
    }

    public void measure() {
        collectStats.run();
    }

    public long getBytesSent() {
        return bytesSent.get();
    }

    public long getBytesReceived() {
        return bytesReceived.get();
    }

    public double getBytesSentPerSecond() {
        return bytesSentPerSecond;
    }

    public double getBytesReceivedPerSecond() {
        return bytesReceivedPerSecond;
    }
}