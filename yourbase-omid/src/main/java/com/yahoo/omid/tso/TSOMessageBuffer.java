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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.Channels;

import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortRequest;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TSOMessageBuffer {

    private static final Log LOG = LogFactory.getLog(TSOMessageBuffer.class);

    private static final int CAPACITY = 20000;
    private static final int THRESHOLD = 32;
    private static final int NBUF = 16;
    private static final int TIMEOUT = 5 * 1000; // Milliseconds

    public static int itWasFull = 0;
    public static int flushes = 0;
    public static long flushed = 0;
    public static int waited = 0;

    private ChannelBuffer buffers[] = new ChannelBuffer[NBUF];
    private ChannelFuture futures[] = new ChannelFuture[NBUF];
    private int currentBuffer;

    private ChannelBuffer buffer;
    private Channel channel;
    private boolean dirty = false;

    public TSOMessageBuffer(Channel channel) {
        this.channel = channel;
        for (int i = 0; i < NBUF; ++i) {
            buffers[i] = ChannelBuffers.directBuffer(CAPACITY);
        }
        currentBuffer = 0;
        buffer = buffers[0];
    }

    public void encode(Object msg) {
        if (buffer.writableBytes() < THRESHOLD) {
            flush();
            ++itWasFull;
        }
        encode(msg, buffer);
        dirty = true;
    }

    public void write(ChannelBuffer copy) {
        if (buffer.writableBytes() < copy.readableBytes()) {
            flush();
            ++itWasFull;
        }
        buffer.writeBytes(copy, copy.readerIndex(), copy.readableBytes());
        dirty = true;
    }

    public void writeCommit(long startTimestamp, long commitTimestamp) {
        if (buffer.writableBytes() < 17) {
            flush();
            ++itWasFull;
        }
        buffer.writeByte(TSOMessage.CommittedTransactionReport);
        buffer.writeLong(startTimestamp);
        buffer.writeLong(commitTimestamp);
        dirty = true;
    }

    public void writeHalfAbort(long startTimestamp) {
        if (buffer.writableBytes() < 9) {
            flush();
            ++itWasFull;
        }
        buffer.writeByte(TSOMessage.AbortedTransactionReport);
        buffer.writeLong(startTimestamp);
        dirty = true;
    }

    public void writeFullAbort(long startTimestamp) {
        if (buffer.writableBytes() < 9) {
            flush();
            ++itWasFull;
        }
        buffer.writeByte(TSOMessage.FullAbortReport);
        buffer.writeLong(startTimestamp);
        dirty = true;
    }

    public void writeLargestIncrease(long largestTimestamp) {
        if (buffer.writableBytes() < 9) {
            flush();
            ++itWasFull;
        }
        buffer.writeByte(TSOMessage.LargestDeletedTimestampReport);
        buffer.writeLong(largestTimestamp);
        dirty = true;
    }

    public static void encode(Object msg, ChannelBuffer buffer) {
        if (msg instanceof TimestampRequest) {
            buffer.writeByte(TSOMessage.TimestampRequest);
        } else if (msg instanceof TimestampResponse) {
            buffer.writeByte(TSOMessage.TimestampResponse);
        } else if (msg instanceof CommitRequest) {
            buffer.writeByte(TSOMessage.CommitRequest);
        } else if (msg instanceof CommitResponse) {
            buffer.writeByte(TSOMessage.CommitResponse);
        } else if (msg instanceof FullAbortRequest) {
            buffer.writeByte(TSOMessage.FullAbortReport);
        } else if (msg instanceof CommitQueryRequest) {
            buffer.writeByte(TSOMessage.CommitQueryRequest);
        } else if (msg instanceof CommitQueryResponse) {
            buffer.writeByte(TSOMessage.CommitQueryResponse);
        } else if (msg instanceof AbortedTransactionReport) {
            buffer.writeByte(TSOMessage.AbortedTransactionReport);
        } else if (msg instanceof CommittedTransactionReport) {
            buffer.writeByte(TSOMessage.CommittedTransactionReport);
        } else if (msg instanceof LargestDeletedTimestampReport) {
            buffer.writeByte(TSOMessage.LargestDeletedTimestampReport);
        } else
            throw new RuntimeException("Wrong obj: " + msg.getClass().getCanonicalName());
        ((TSOMessage) msg).writeObject(buffer);
    }

    public void flush() {
        if (!dirty) {
            return;
        }
        //      if (buffer.readableBytes() > 5000) {
        //      LOG.trace("Flushing " + buffer.readableBytes() + " bytes");
        //      }
        ++flushes;
        flushed += buffer.readableBytes();
        futures[currentBuffer] = Channels.write(channel, buffer);
        ++currentBuffer;
        currentBuffer %= NBUF;
        if (futures[currentBuffer] != null) {
            if (!futures[currentBuffer].isDone()) {
                ++waited;
                if (!futures[currentBuffer].awaitUninterruptibly(TIMEOUT)) {
                    channel.close();
                    LOG.info("Buffer is full, closing connection with client");
                }
            }
        }
        buffer = buffers[currentBuffer];
        buffer.clear();
        dirty = false;
    }
}
