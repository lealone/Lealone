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

package com.codefollower.lealone.omid.tso;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.codefollower.lealone.omid.replication.SharedMessageBuffer.ReadingBuffer;
import com.codefollower.lealone.omid.tso.messages.AbortRequest;
import com.codefollower.lealone.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CommitQueryRequest;
import com.codefollower.lealone.omid.tso.messages.CommitQueryResponse;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.FullAbortRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;
import com.codefollower.lealone.omid.tso.persistence.LoggerException;
import com.codefollower.lealone.omid.tso.persistence.LoggerProtocol;
import com.codefollower.lealone.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;
import com.codefollower.lealone.omid.tso.persistence.LoggerException.Code;

/**
 * ChannelHandler for the TSO Server
 * @author maysam
 *
 */
public class TSOHandler extends SimpleChannelHandler {

    private static final Log LOG = LogFactory.getLog(TSOHandler.class);

    /**
     * Bytes monitor
     */
    public static final AtomicInteger transferredBytes = new AtomicInteger();
    //   public static int transferredBytes = 0;
    public static int abortCount = 0;
    public static int hitCount = 0;
    public static long queries = 0;

    /**
     * Channel Group
     */
    private ChannelGroup channelGroup = null;

    private Map<Channel, ReadingBuffer> messageBuffersMap = new HashMap<Channel, ReadingBuffer>();

    /**
     * Timestamp Oracle
     */
    private TimestampOracle timestampOracle = null;

    /**
     * The wrapper for the shared state of TSO
     */
    private TSOState sharedState;

    private FlushThread flushThread;
    private ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> flushFuture;

    private ExecutorService executor;

    /**
     * Constructor
     * @param channelGroup
     */
    public TSOHandler(ChannelGroup channelGroup, TSOState state) {
        this.channelGroup = channelGroup;
        this.timestampOracle = state.getTimestampOracle();
        this.sharedState = state;
    }

    public void start() {
        this.flushThread = new FlushThread();
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(Thread.currentThread().getThreadGroup(), r);
                t.setDaemon(true);
                t.setName("Flush Thread");
                return t;
            }
        });
        this.flushFuture = scheduledExecutor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
        this.executor = Executors.newSingleThreadExecutor();
    }

    /**
     * Returns the number of transferred bytes
     * @return the number of transferred bytes
     */
    public static long getTransferredBytes() {
        return transferredBytes.longValue();
    }

    /**
     * If write of a message was not possible before, we can do it here
     */
    @Override
    public void channelInterestChanged(ChannelHandlerContext ctx, ChannelStateEvent e) {
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        channelGroup.add(ctx.getChannel());
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        synchronized (sharedMsgBufLock) {
            sharedState.sharedMessageBuffer.removeReadingBuffer(ctx);
        }
    }

    /**
     * Handle receieved messages
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        Object msg = e.getMessage();
        if (msg instanceof TimestampRequest) {
            handle((TimestampRequest) msg, ctx);
            return;
        } else if (msg instanceof CommitRequest) {
            handle((CommitRequest) msg, ctx);
            return;
        } else if (msg instanceof FullAbortRequest) {
            handle((FullAbortRequest) msg, ctx);
            return;
        } else if (msg instanceof CommitQueryRequest) {
            handle((CommitQueryRequest) msg, ctx);
            return;
        }
    }

    public void handle(AbortRequest msg, ChannelHandlerContext ctx) {
        synchronized (sharedState) {
            DataOutputStream toWAL = sharedState.toWAL;
            try {
                toWAL.writeByte(LoggerProtocol.ABORT);
                toWAL.writeLong(msg.startTimestamp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            abortCount++;
            sharedState.processAbort(msg.startTimestamp);
            synchronized (sharedMsgBufLock) {
                queueHalfAbort(msg.startTimestamp);
            }
        }
    }

    /**
     * Handle the TimestampRequest message
     */
    public void handle(TimestampRequest msg, ChannelHandlerContext ctx) {
        long timestamp;
        synchronized (sharedState) {
            try {
                timestamp = timestampOracle.next(sharedState.toWAL);
            } catch (IOException e) {
                LOG.error("failed to return the next timestamp", e);
                return;
            }
        }

        ReadingBuffer buffer;
        Channel channel = ctx.getChannel();
        boolean bootstrap = false;
        synchronized (messageBuffersMap) {
            buffer = messageBuffersMap.get(ctx.getChannel());
            if (buffer == null) {
                synchronized (sharedMsgBufLock) {
                    bootstrap = true;
                    buffer = sharedState.sharedMessageBuffer.getReadingBuffer(ctx);
                    messageBuffersMap.put(channel, buffer);
                    channelGroup.add(channel);
                    LOG.warn("Channel connected: " + messageBuffersMap.size());
                }
            }
        }
        if (bootstrap) {
            synchronized (sharedState) {
                synchronized (sharedMsgBufLock) {
                    channel.write(buffer.getZipperState());
                    buffer.initializeIndexes();
                }
            }
            for (AbortedTransaction halfAborted : sharedState.hashmap.halfAborted) {
                channel.write(new AbortedTransactionReport(halfAborted.getStartTimestamp()));
            }
        }
        ChannelBuffer cb;
        ChannelFuture future = Channels.future(channel);
        synchronized (sharedMsgBufLock) {
            cb = buffer.flush(future);
        }
        Channels.write(ctx, future, cb);
        Channels.write(channel, new TimestampResponse(timestamp));
    }

    ChannelBuffer cb = ChannelBuffers.buffer(10);

    private boolean finish;

    public static long waitTime = 0;
    public static long commitTime = 0;
    public static long checkTime = 0;

    private Object sharedMsgBufLock = new Object();
    private Object callbackLock = new Object();
    private AddRecordCallback noCallback = new AddRecordCallback() {
        @Override
        public void addRecordComplete(int rc, Object ctx) {
        }
    };

    private Runnable createAbortedSnaphostTask = new Runnable() {
        @Override
        public void run() {
            createAbortedSnapshot();
        }
    };

    public void createAbortedSnapshot() {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream toWAL = new DataOutputStream(baos);

        long snapshot = sharedState.hashmap.getAndIncrementAbortedSnapshot();

        try {
            toWAL.writeByte(LoggerProtocol.SNAPSHOT);
            toWAL.writeLong(snapshot);
            for (AbortedTransaction aborted : sharedState.hashmap.halfAborted) {
                // ignore aborted transactions from last snapshot
                if (aborted.getSnapshot() < snapshot) {
                    toWAL.writeByte(LoggerProtocol.ABORT);
                    toWAL.writeLong(aborted.getStartTimestamp());
                }
            }
        } catch (IOException e) {
            // can't happen
            throw new RuntimeException(e);
        }

        sharedState.addRecord(baos.toByteArray(), noCallback, null);
    }

    /**
     * Handle the CommitRequest message
     */
    public void handle(CommitRequest msg, ChannelHandlerContext ctx) {
        CommitResponse reply = new CommitResponse(msg.startTimestamp);
        ByteArrayOutputStream baos = sharedState.baos;
        DataOutputStream toWAL = sharedState.toWAL;
        synchronized (sharedState) {
            //0. check if it should abort
            if (msg.startTimestamp < timestampOracle.first()) {
                reply.committed = false;
                LOG.warn("Aborting transaction after restarting TSO");
            } else if (msg.startTimestamp < sharedState.largestDeletedTimestamp) {
                // Too old
                reply.committed = false;//set as abort
                LOG.warn("Too old starttimestamp: ST " + msg.startTimestamp + " MAX " + sharedState.largestDeletedTimestamp);
            } else {
                //1. check the write-write conflicts
                for (RowKey r : msg.rows) {
                    long value;
                    value = sharedState.hashmap.get(r.getRow(), r.getTable(), r.hashCode());
                    if (value != 0 && value > msg.startTimestamp) {
                        reply.committed = false;//set as abort
                        break;
                    } else if (value == 0 && sharedState.largestDeletedTimestamp > msg.startTimestamp) {
                        //then it could have been committed after start timestamp but deleted by recycling
                        LOG.warn("Old transaction {Start timestamp  " + msg.startTimestamp + "} {Largest deleted timestamp "
                                + sharedState.largestDeletedTimestamp + "}");
                        reply.committed = false;//set as abort
                        break;
                    }
                }
            }

            if (reply.committed) {
                //2. commit
                try {
                    long commitTimestamp = timestampOracle.next(toWAL);
                    sharedState.uncommited.commit(commitTimestamp);
                    sharedState.uncommited.commit(msg.startTimestamp);
                    reply.commitTimestamp = commitTimestamp;
                    if (msg.rows.length > 0) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Adding commit to WAL");
                        }
                        toWAL.writeByte(LoggerProtocol.COMMIT);
                        toWAL.writeLong(msg.startTimestamp);
                        toWAL.writeLong(commitTimestamp);

                        long oldLargestDeletedTimestamp = sharedState.largestDeletedTimestamp;

                        for (RowKey r : msg.rows) {
                            sharedState.largestDeletedTimestamp = sharedState.hashmap.put(r.getRow(), r.getTable(),
                                    commitTimestamp, r.hashCode(), oldLargestDeletedTimestamp);
                        }

                        sharedState.processCommit(msg.startTimestamp, commitTimestamp);
                        if (sharedState.largestDeletedTimestamp > oldLargestDeletedTimestamp) {
                            toWAL.writeByte(LoggerProtocol.LARGEST_DELETED_TIMESTAMP);
                            toWAL.writeLong(sharedState.largestDeletedTimestamp);
                            Set<Long> toAbort = sharedState.uncommited
                                    .raiseLargestDeletedTransaction(sharedState.largestDeletedTimestamp);
                            if (LOG.isWarnEnabled() && !toAbort.isEmpty()) {
                                LOG.warn("Slow transactions after raising max: " + toAbort.size());
                            }
                            synchronized (sharedMsgBufLock) {
                                for (Long id : toAbort) {
                                    sharedState.hashmap.setHalfAborted(id);
                                    queueHalfAbort(id);
                                }
                                queueLargestIncrease(sharedState.largestDeletedTimestamp);
                            }
                        }
                        if (sharedState.largestDeletedTimestamp > sharedState.previousLargestDeletedTimestamp
                                + TSOState.MAX_ITEMS) {
                            // schedule snapshot
                            executor.submit(createAbortedSnaphostTask);
                            sharedState.previousLargestDeletedTimestamp = sharedState.largestDeletedTimestamp;
                        }
                        synchronized (sharedMsgBufLock) {
                            queueCommit(msg.startTimestamp, commitTimestamp);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else { //add it to the aborted list
                abortCount++;
                try {
                    toWAL.writeByte(LoggerProtocol.ABORT);
                    toWAL.writeLong(msg.startTimestamp);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                sharedState.processAbort(msg.startTimestamp);

                synchronized (sharedMsgBufLock) {
                    queueHalfAbort(msg.startTimestamp);
                }
            }

            TSOHandler.transferredBytes.incrementAndGet();

            ChannelAndMessage cam = new ChannelAndMessage(ctx, reply);

            sharedState.nextBatch.add(cam);
            if (sharedState.baos.size() >= TSOState.BATCH_SIZE) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Going to add record of size " + sharedState.baos.size());
                }
                //sharedState.lh.asyncAddEntry(baos.toByteArray(), this, sharedState.nextBatch);
                sharedState.addRecord(baos.toByteArray(), new AddRecordCallback() {
                    @Override
                    public void addRecordComplete(int rc, Object ctx) {
                        if (rc != Code.OK) {
                            LOG.warn("Write failed: " + LoggerException.getMessage(rc));

                        } else {
                            synchronized (callbackLock) {
                                @SuppressWarnings("unchecked")
                                ArrayList<ChannelAndMessage> theBatch = (ArrayList<ChannelAndMessage>) ctx;
                                for (ChannelAndMessage cam : theBatch) {
                                    Channels.write(cam.ctx, Channels.succeededFuture(cam.ctx.getChannel()), cam.msg);
                                }
                            }

                        }
                    }
                }, sharedState.nextBatch);
                sharedState.nextBatch = new ArrayList<ChannelAndMessage>(sharedState.nextBatch.size() + 5);
                sharedState.baos.reset();
            }

        }

    }

    /**
     * Handle the CommitQueryRequest message
     */
    public void handle(CommitQueryRequest msg, ChannelHandlerContext ctx) {
        CommitQueryResponse reply = new CommitQueryResponse(msg.startTimestamp);
        reply.queryTimestamp = msg.queryTimestamp;
        synchronized (sharedState) {
            queries++;
            //1. check the write-write conflicts
            long value;
            value = sharedState.hashmap.getCommittedTimestamp(msg.queryTimestamp);
            if (value != 0) { //it exists
                reply.commitTimestamp = value;
                reply.committed = value < msg.startTimestamp;//set as abort
            } else if (sharedState.hashmap.isHalfAborted(msg.queryTimestamp))
                reply.committed = false;
            else if (sharedState.uncommited.isUncommited(msg.queryTimestamp))
                reply.committed = false;
            else
                reply.retry = true;
            //         else if (sharedState.largestDeletedTimestamp >= msg.queryTimestamp) 
            //            reply.committed = true;
            // TODO retry needed? isnt it just fully aborted?

            ctx.getChannel().write(reply);

            // We send the message directly. If after a failure the state is inconsistent we'll detect it

        }
    }

    public void flush() {
        synchronized (sharedState) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Adding record, size: " + sharedState.baos.size());
            }
            sharedState.addRecord(sharedState.baos.toByteArray(), new AddRecordCallback() {
                @Override
                public void addRecordComplete(int rc, Object ctx) {
                    if (rc != Code.OK) {
                        LOG.warn("Write failed: " + LoggerException.getMessage(rc));

                    } else {
                        synchronized (callbackLock) {
                            @SuppressWarnings("unchecked")
                            ArrayList<ChannelAndMessage> theBatch = (ArrayList<ChannelAndMessage>) ctx;
                            for (ChannelAndMessage cam : theBatch) {
                                Channels.write(cam.ctx, Channels.succeededFuture(cam.ctx.getChannel()), cam.msg);
                            }
                        }

                    }
                }
            }, sharedState.nextBatch);
            sharedState.nextBatch = new ArrayList<ChannelAndMessage>(sharedState.nextBatch.size() + 5);
            sharedState.baos.reset();
            if (flushFuture.cancel(false)) {
                flushFuture = scheduledExecutor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
            }
        }
    }

    public class FlushThread implements Runnable {
        @Override
        public void run() {
            if (finish) {
                return;
            }
            if (sharedState.nextBatch.size() > 0) {
                synchronized (sharedState) {
                    if (sharedState.nextBatch.size() > 0) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace("Flushing log batch.");
                        }
                        flush();
                    }
                }
            }
            flushFuture = scheduledExecutor.schedule(flushThread, TSOState.FLUSH_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    private void queueCommit(long startTimestamp, long commitTimestamp) {
        sharedState.sharedMessageBuffer.writeCommit(startTimestamp, commitTimestamp);
    }

    private void queueHalfAbort(long startTimestamp) {
        sharedState.sharedMessageBuffer.writeHalfAbort(startTimestamp);
    }

    private void queueFullAbort(long startTimestamp) {
        sharedState.sharedMessageBuffer.writeFullAbort(startTimestamp);
    }

    private void queueLargestIncrease(long largestTimestamp) {
        sharedState.sharedMessageBuffer.writeLargestIncrease(largestTimestamp);
    }

    /**
     * Handle the FullAbortReport message
     */
    public void handle(FullAbortRequest msg, ChannelHandlerContext ctx) {
        synchronized (sharedState) {
            DataOutputStream toWAL = sharedState.toWAL;
            try {
                toWAL.writeByte(LoggerProtocol.FULLABORT);
                toWAL.writeLong(msg.startTimestamp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            sharedState.processFullAbort(msg.startTimestamp);
        }
        synchronized (sharedMsgBufLock) {
            queueFullAbort(msg.startTimestamp);
        }
    }

    /*
     * Wrapper for Channel and Message
     */
    public static class ChannelAndMessage {
        ChannelHandlerContext ctx;
        TSOMessage msg;

        ChannelAndMessage(ChannelHandlerContext c, TSOMessage m) {
            ctx = c;
            msg = m;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        LOG.warn("TSOHandler: Unexpected exception from downstream.", e.getCause());
        Channels.close(e.getChannel());
    }

    public void stop() {
        finish = true;
    }

}
