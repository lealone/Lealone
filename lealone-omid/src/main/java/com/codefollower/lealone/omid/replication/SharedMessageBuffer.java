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

package com.codefollower.lealone.omid.replication;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;

public class SharedMessageBuffer {
    private static final Log LOG = LogFactory.getLog(SharedMessageBuffer.class);
    private static final int MAX_MESSAGE_SIZE = 30;

    private final BlockingQueue<ReadersAwareBuffer> readersAwareBufferPool = new LinkedBlockingQueue<ReadersAwareBuffer>();
    private final Zipper zipper = new Zipper();
    private final Set<ReadingBuffer> readingBuffers = new TreeSet<SharedMessageBuffer.ReadingBuffer>();

    private ReadersAwareBuffer pastReadersAwareBuffer = new ReadersAwareBuffer();
    private ReadersAwareBuffer currentReadersAwareBuffer = new ReadersAwareBuffer();
    private ChannelBuffer writeChannelBuffer = currentReadersAwareBuffer.buffer;

    public class ReadingBuffer implements Comparable<ReadingBuffer> {
        private final ChannelHandlerContext ctx;
        private final Channel channel;

        private ReadersAwareBuffer readingReadersAwareBuffer;
        private ChannelBuffer readChannelBuffer;
        private int readerIndex = 0;

        private ReadingBuffer(ChannelHandlerContext ctx) {
            this.ctx = ctx;
            this.channel = ctx.getChannel();
        }

        public void initializeIndexes() {
            this.readingReadersAwareBuffer = currentReadersAwareBuffer;
            this.readChannelBuffer = readingReadersAwareBuffer.buffer;
            this.readerIndex = readChannelBuffer.writerIndex();
        }

        /**
         * Computes and returns the deltaSO for the associated client.
         * 
         * This function registers some callbacks in the future passed for cleanup purposes, so the ChannelFuture object
         * must be notified after the communication is finished.
         * 
         * @param future
         *            It registers some callbacks on it
         * @return the deltaSO for the associated client
         */
        public ChannelBuffer flush(ChannelFuture future) {
            int readable = readChannelBuffer.readableBytes() - readerIndex;

            if (readable == 0 && readingReadersAwareBuffer != pastReadersAwareBuffer) {
                return ChannelBuffers.EMPTY_BUFFER;
            }

            ChannelBuffer deltaSO = readChannelBuffer.slice(readerIndex, readable);
            addFinishedWriteListener(future, readingReadersAwareBuffer);
            readingReadersAwareBuffer.increaseReaders();
            readerIndex += readable;
            if (readingReadersAwareBuffer == pastReadersAwareBuffer) {
                readingReadersAwareBuffer = currentReadersAwareBuffer;
                readChannelBuffer = readingReadersAwareBuffer.buffer;
                readerIndex = 0;
                readable = readChannelBuffer.readableBytes();
                deltaSO = ChannelBuffers.wrappedBuffer(deltaSO, readChannelBuffer.slice(readerIndex, readable));
                addFinishedWriteListener(future, readingReadersAwareBuffer);
                readingReadersAwareBuffer.increaseReaders();
                readerIndex += readable;
            }

            return deltaSO;
        }

        private void addFinishedWriteListener(ChannelFuture future, final ReadersAwareBuffer buffer) {
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    buffer.decreaseReaders();
                    if (buffer.isReadyForPool()) {
                        buffer.reset();
                        readersAwareBufferPool.add(buffer);
                    }
                }
            });
        }

        public ZipperState getZipperState() {
            return zipper.getZipperState();
        }

        @Override
        public int compareTo(ReadingBuffer o) {
            return this.channel.compareTo(o.channel);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReadingBuffer))
                return false;
            ReadingBuffer buf = (ReadingBuffer) obj;
            return this.channel.equals(buf.channel);
        }

    }

    public ReadingBuffer getReadingBuffer(ChannelHandlerContext ctx) {
        ReadingBuffer rb = new ReadingBuffer(ctx);
        readingBuffers.add(rb);
        return rb;
    }

    public void removeReadingBuffer(ChannelHandlerContext ctx) {
        readingBuffers.remove(new ReadingBuffer(ctx));
    }

    public void writeCommit(long startTimestamp, long commitTimestamp) {
        checkBufferSpace();
        zipper.encodeCommit(writeChannelBuffer, startTimestamp, commitTimestamp);
    }

    public void writeHalfAbort(long startTimestamp) {
        checkBufferSpace();
        zipper.encodeHalfAbort(writeChannelBuffer, startTimestamp);
    }

    public void writeFullAbort(long startTimestamp) {
        checkBufferSpace();
        zipper.encodeFullAbort(writeChannelBuffer, startTimestamp);
    }

    public void writeLargestDeletedTimestamp(long largestDeletedTimestamp) {
        checkBufferSpace();
        zipper.encodeLargestDeletedTimestamp(writeChannelBuffer, largestDeletedTimestamp);
    }

    private void checkBufferSpace() {
        if (writeChannelBuffer.writableBytes() < MAX_MESSAGE_SIZE) {
            nextBuffer();
        }
    }

    private void nextBuffer() {
        if (LOG.isDebugEnabled())
            LOG.debug("Switching buffers");

        // mark past buffer as scheduled for pool when all pending operations finish
        pastReadersAwareBuffer.scheduleForPool();

        for (final ReadingBuffer rb : readingBuffers) {
            if (rb.readingReadersAwareBuffer == pastReadersAwareBuffer) {
                ChannelFuture future = Channels.future(rb.channel);
                ChannelBuffer cb = rb.flush(future);
                Channels.write(rb.ctx, future, cb);
            }
        }

        pastReadersAwareBuffer = currentReadersAwareBuffer;
        currentReadersAwareBuffer = readersAwareBufferPool.poll();
        if (currentReadersAwareBuffer == null) {
            currentReadersAwareBuffer = new ReadersAwareBuffer();
            LOG.warn("Allocated a new ReadersAwareBuffer");
        }
        writeChannelBuffer = currentReadersAwareBuffer.buffer;
    }
}
