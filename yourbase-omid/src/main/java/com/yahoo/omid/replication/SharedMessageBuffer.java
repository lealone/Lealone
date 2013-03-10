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

package com.yahoo.omid.replication;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedMessageBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(SharedMessageBuffer.class);

    private static final int MAX_MESSAGE_SIZE = 30;

    ReadersAwareBuffer pastBuffer = new ReadersAwareBuffer();
    ReadersAwareBuffer currentBuffer = new ReadersAwareBuffer();
    ChannelBuffer writeBuffer = currentBuffer.buffer;
    BlockingQueue<ReadersAwareBuffer> bufferPool = new LinkedBlockingQueue<ReadersAwareBuffer>();
    Zipper zipper = new Zipper();
    Set<ReadingBuffer> readingBuffers = new TreeSet<SharedMessageBuffer.ReadingBuffer>();

    public class ReadingBuffer implements Comparable<ReadingBuffer> {
        private ChannelBuffer readBuffer;
        private int readerIndex = 0;
        private ReadersAwareBuffer readingBuffer;
        private ChannelHandlerContext ctx;
        private Channel channel;

        private ReadingBuffer(ChannelHandlerContext ctx) {
            this.channel = ctx.getChannel();
            this.ctx = ctx;
        }

        public void initializeIndexes() {
            this.readingBuffer = currentBuffer;
            this.readBuffer = readingBuffer.buffer;
            this.readerIndex = readBuffer.writerIndex();
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
            int readable = readBuffer.readableBytes() - readerIndex;

            if (readable == 0 && readingBuffer != pastBuffer) {
                return ChannelBuffers.EMPTY_BUFFER;
            }

            ChannelBuffer deltaSO = readBuffer.slice(readerIndex, readable);
            addFinishedWriteListener(future, readingBuffer);
            readingBuffer.increaseReaders();
            readerIndex += readable;
            if (readingBuffer == pastBuffer) {
                readingBuffer = currentBuffer;
                readBuffer = readingBuffer.buffer;
                readerIndex = 0;
                readable = readBuffer.readableBytes();
                deltaSO = ChannelBuffers.wrappedBuffer(deltaSO, readBuffer.slice(readerIndex, readable));
                addFinishedWriteListener(future, readingBuffer);
                readingBuffer.increaseReaders();
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
                        bufferPool.add(buffer);
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
        zipper.encodeCommit(writeBuffer, startTimestamp, commitTimestamp);
    }

    public void writeHalfAbort(long startTimestamp) {
        checkBufferSpace();
        zipper.encodeHalfAbort(writeBuffer, startTimestamp);
    }

    public void writeFullAbort(long startTimestamp) {
        checkBufferSpace();
        zipper.encodeFullAbort(writeBuffer, startTimestamp);
    }

    public void writeLargestIncrease(long largestTimestamp) {
        checkBufferSpace();
        zipper.encodeLargestIncrease(writeBuffer, largestTimestamp);
    }

    private void checkBufferSpace() {
        if (writeBuffer.writableBytes() < MAX_MESSAGE_SIZE) {
            nextBuffer();
        }
    }

    private void nextBuffer() {
        LOG.debug("Switching buffers");

        // mark past buffer as scheduled for pool when all pending operations finish
        pastBuffer.scheduleForPool();

        for (final ReadingBuffer rb : readingBuffers) {
            if (rb.readingBuffer == pastBuffer) {
                ChannelFuture future = Channels.future(rb.channel);
                ChannelBuffer cb = rb.flush(future);
                Channels.write(rb.ctx, future, cb);
            }
        }

        pastBuffer = currentBuffer;
        currentBuffer = bufferPool.poll();
        if (currentBuffer == null) {
            currentBuffer = new ReadersAwareBuffer();
        }
        writeBuffer = currentBuffer.buffer;
    }
}
