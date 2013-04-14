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

package com.codefollower.lealone.omid.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;

import com.codefollower.lealone.omid.replication.Zipper;
import com.codefollower.lealone.omid.replication.ZipperState;
import com.codefollower.lealone.omid.tso.Committed;
import com.codefollower.lealone.omid.tso.RowKey;
import com.codefollower.lealone.omid.tso.TSOMessage;
import com.codefollower.lealone.omid.tso.messages.AbortRequest;
import com.codefollower.lealone.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CleanedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CommitQueryRequest;
import com.codefollower.lealone.omid.tso.messages.CommitQueryResponse;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.FullAbortRequest;
import com.codefollower.lealone.omid.tso.messages.LargestDeletedTimestampReport;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;
import com.codefollower.lealone.omid.tso.serialization.TSODecoder;
import com.codefollower.lealone.omid.tso.serialization.TSOEncoder;

public class TSOClient extends SimpleChannelHandler {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    public enum Result {
        OK, ABORTED
    };

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, RETRY_CONNECT_WAIT
    };

    private Queue<CreateCallback> createCallbacks;
    private Map<Long, CommitCallback> commitCallbacks;
    private Map<Long, List<CommitQueryCallback>> isCommittedCallbacks;

    private Committed committed = new Committed();
    private Set<Long> aborted = Collections.synchronizedSet(new HashSet<Long>(1000));
    private long largestDeletedTimestamp;
    private long connectionTimestamp = 0;
    private boolean hasConnectionTimestamp = false;

    private ChannelFactory factory;
    private ClientBootstrap bootstrap;
    private Channel channel;
    private InetSocketAddress addr;
    private int maxRetries;
    private int retries;
    private int retryDelayMs;
    private Timer retryTimer;

    private ArrayBlockingQueue<Op> queuedOps;
    private State state;

    private interface Op {
        public void execute(Channel channel);

        public void error(Exception e);
    }

    private class AbortOp implements Op {
        long transactionId;

        AbortOp(long transactionid) throws IOException {
            this.transactionId = transactionid;
        }

        public void execute(Channel channel) {
            try {
                synchronized (commitCallbacks) {
                    if (commitCallbacks.containsKey(transactionId)) {
                        throw new IOException("Already committing transaction " + transactionId);
                    }
                }

                AbortRequest ar = new AbortRequest();
                ar.startTimestamp = transactionId;
                ChannelFuture f = channel.write(ar);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
        }

        public void error(Exception e) {
        }
    }

    private class NewTimestampOp implements Op {
        private CreateCallback cb;

        NewTimestampOp(CreateCallback cb) {
            this.cb = cb;
        }

        public void execute(Channel channel) {
            try {
                synchronized (createCallbacks) {
                    createCallbacks.add(cb);
                }

                TimestampRequest tr = new TimestampRequest();
                ChannelFuture f = channel.write(tr);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
        }

        public void error(Exception e) {
            synchronized (createCallbacks) {
                createCallbacks.remove();
            }

            cb.error(e);
        }
    }

    private class CommitQueryOp implements Op {
        long startTimestamp;
        long queryTimestamp;
        CommitQueryCallback cb;

        CommitQueryOp(long startTimestamp, long queryTimestamp, CommitQueryCallback cb) {
            this.startTimestamp = startTimestamp;
            this.queryTimestamp = queryTimestamp;
            this.cb = cb;
        }

        public void execute(Channel channel) {
            try {
                synchronized (isCommittedCallbacks) {
                    List<CommitQueryCallback> callbacks = isCommittedCallbacks.get(startTimestamp);
                    if (callbacks == null) {
                        callbacks = new ArrayList<CommitQueryCallback>(1);
                    }
                    callbacks.add(cb);
                    isCommittedCallbacks.put(startTimestamp, callbacks);
                }

                CommitQueryRequest qr = new CommitQueryRequest(startTimestamp, queryTimestamp);
                ChannelFuture f = channel.write(qr);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
        }

        public void error(Exception e) {
            synchronized (isCommittedCallbacks) {
                isCommittedCallbacks.remove(startTimestamp);
            }

            cb.error(e);
        }
    }

    private class CommitOp implements Op {
        long transactionId;
        RowKey[] rows;
        CommitCallback cb;

        CommitOp(long transactionid, RowKey[] rows, CommitCallback cb) throws IOException {
            this.transactionId = transactionid;
            this.rows = rows;
            this.cb = cb;
        }

        public void execute(Channel channel) {
            try {
                synchronized (commitCallbacks) {
                    if (commitCallbacks.containsKey(transactionId)) {
                        throw new IOException("Already committing transaction " + transactionId);
                    }
                    commitCallbacks.put(transactionId, cb);
                }

                CommitRequest cr = new CommitRequest();
                cr.startTimestamp = transactionId;
                cr.rows = rows;
                ChannelFuture f = channel.write(cr);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }
        }

        public void error(Exception e) {
            synchronized (commitCallbacks) {
                commitCallbacks.remove(transactionId);
            }
            cb.error(e);
        }
    }

    private class AbortCompleteOp implements Op {
        long transactionId;
        AbortCompleteCallback cb;

        AbortCompleteOp(long transactionId, AbortCompleteCallback cb) throws IOException {
            this.transactionId = transactionId;
            this.cb = cb;
        }

        public void execute(Channel channel) {
            try {
                FullAbortRequest far = new FullAbortRequest();
                far.startTimestamp = transactionId;

                ChannelFuture f = channel.write(far);
                f.addListener(new ChannelFutureListener() {
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            error(new IOException("Error writing to socket"));
                        } else {
                            cb.complete();
                        }
                    }
                });
            } catch (Exception e) {
                error(e);
            }

        }

        public void error(Exception e) {
            cb.error(e);
        }
    }

    public TSOClient(Configuration conf) throws IOException {
        state = State.DISCONNECTED;
        queuedOps = new ArrayBlockingQueue<Op>(200);
        retryTimer = new Timer(true);

        commitCallbacks = Collections.synchronizedMap(new HashMap<Long, CommitCallback>());
        isCommittedCallbacks = Collections.synchronizedMap(new HashMap<Long, List<CommitQueryCallback>>());
        createCallbacks = new ConcurrentLinkedQueue<CreateCallback>();
        channel = null;

        LOG.info("Starting TSOClient");

        // Start client with Nb of active threads = 3 as maximum.
        factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 3);
        // Create the bootstrap
        bootstrap = new ClientBootstrap(factory);

        int executorThreads = conf.getInt("tso.executor.threads", 3);

        bootstrap.getPipeline().addLast("executor",
                new ExecutionHandler(new OrderedMemoryAwareThreadPoolExecutor(executorThreads, 1024 * 1024, 4 * 1024 * 1024)));
        bootstrap.getPipeline().addLast("handler", this);
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);

        String host = conf.get("tso.host");
        int port = conf.getInt("tso.port", 1234);
        maxRetries = conf.getInt("tso.max_retries", 100);
        retryDelayMs = conf.getInt("tso.retry_delay_ms", 1000);

        if (host == null) {
            throw new IOException("tso.host missing from configuration");
        }

        addr = new InetSocketAddress(host, port);
        connectIfNeeded();
    }

    private State connectIfNeeded() throws IOException {
        if (state == State.CONNECTED) { //多数情况下都是CONNECTED，所以在synchronized前检测一下，避免不必要的同步
            return state;
        }
        synchronized (state) {
            if (state == State.CONNECTED || state == State.CONNECTING) {
                return state;
            }
            if (state == State.RETRY_CONNECT_WAIT) {
                return State.CONNECTING;
            }

            if (retries > maxRetries) {
                IOException e = new IOException("Max connection retries exceeded");
                bailout(e);
                throw e;
            }
            retries++;
            bootstrap.connect(addr).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (LOG.isDebugEnabled())
                        LOG.debug("Connection completed. Success: " + future.isSuccess());
                }
            });
            state = State.CONNECTING;
            return state;
        }
    }

    private void withConnection(Op op) throws IOException {
        State state = connectIfNeeded();

        if (state == State.CONNECTING) {
            try {
                queuedOps.put(op);
            } catch (InterruptedException e) {
                throw new IOException("Couldn't add new operation", e);
            }
        } else if (state == State.CONNECTED) {
            op.execute(channel);
        } else {
            throw new IOException("Invalid connection state " + state);
        }
    }

    public void getNewTimestamp(CreateCallback cb) throws IOException {
        withConnection(new NewTimestampOp(cb));
    }

    public void isCommitted(long startTimestamp, long queryTimestamp, CommitQueryCallback cb) throws IOException {
        withConnection(new CommitQueryOp(startTimestamp, queryTimestamp, cb));
    }

    public void abort(long transactionId) throws IOException {
        withConnection(new AbortOp(transactionId));
    }

    public void commit(long transactionId, RowKey[] rows, CommitCallback cb) throws IOException {
        withConnection(new CommitOp(transactionId, rows, cb));
    }

    public void completeAbort(long transactionId, AbortCompleteCallback cb) throws IOException {
        withConnection(new AbortCompleteOp(transactionId, cb));
    }

    @Override
    synchronized public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        e.getChannel().getPipeline().addFirst("decoder", new TSODecoder(new Zipper()));
        e.getChannel().getPipeline().addAfter("decoder", "encoder", new TSOEncoder());
    }

    /**
     * Starts the traffic
     */
    @Override
    synchronized public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) {
        synchronized (state) {
            channel = e.getChannel();
            state = State.CONNECTED;
            retries = 0;
        }
        clearState();
        if (LOG.isDebugEnabled())
            LOG.debug("Channel connected");
        Op o = queuedOps.poll();
        while (o != null && state == State.CONNECTED) {
            o.execute(channel);
            o = queuedOps.poll();
        }
    }

    private void clearState() {
        committed = new Committed();
        aborted.clear();
        largestDeletedTimestamp = 0;
        connectionTimestamp = 0;
        hasConnectionTimestamp = false;
    }

    @Override
    synchronized public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        synchronized (state) {
            if (LOG.isDebugEnabled())
                LOG.debug("Channel disconnected");
            channel = null;
            state = State.DISCONNECTED;
            for (CreateCallback cb : createCallbacks) {
                cb.error(new IOException("Channel Disconnected"));
            }
            for (CommitCallback cb : commitCallbacks.values()) {
                cb.error(new IOException("Channel Disconnected"));
            }
            for (List<CommitQueryCallback> lcqb : isCommittedCallbacks.values()) {
                for (CommitQueryCallback cqb : lcqb) {
                    cqb.error(new IOException("Channel Disconnected"));
                }
            }
            createCallbacks.clear();
            commitCallbacks.clear();
            isCommittedCallbacks.clear();
            connectIfNeeded();
        }
    }

    public boolean validRead(long queryTimestamp, long startTimestamp) throws IOException {
        if (queryTimestamp == startTimestamp)
            return true;
        if (aborted.contains(queryTimestamp))
            return false;
        long commitTimestamp = committed.getCommit(queryTimestamp);
        if (commitTimestamp != -1)
            return commitTimestamp <= startTimestamp;
        if (hasConnectionTimestamp && queryTimestamp > connectionTimestamp)
            return queryTimestamp <= largestDeletedTimestamp;
        if (queryTimestamp <= largestDeletedTimestamp)
            return true;

        SyncCommitQueryCallback cb = new SyncCommitQueryCallback();
        isCommitted(startTimestamp, queryTimestamp, cb);
        try {
            cb.await();
        } catch (InterruptedException e) {
            throw new IOException("Commit query didn't complete", e);
        }
        return cb.isCommitted();
    }

    /**
     * When a message is received, handle it based on its type
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("messageReceived " + e.getMessage());
        }
        Object msg = e.getMessage();
        if (msg instanceof CommitResponse) {
            CommitResponse r = (CommitResponse) msg;
            CommitCallback cb = null;
            synchronized (commitCallbacks) {
                cb = commitCallbacks.remove(r.startTimestamp);
            }
            if (cb == null) {
                LOG.error("Received a commit response for a nonexisting commit");
                return;
            }
            cb.complete(r.committed ? Result.OK : Result.ABORTED, r.commitTimestamp);
        } else if (msg instanceof TimestampResponse) {
            CreateCallback cb = createCallbacks.poll();
            long timestamp = ((TimestampResponse) msg).timestamp;
            if (!hasConnectionTimestamp || timestamp < connectionTimestamp) {
                hasConnectionTimestamp = true;
                connectionTimestamp = timestamp;
            }
            if (cb == null) {
                LOG.error("Receiving a timestamp response, but none requested: " + timestamp);
                return;
            }
            cb.complete(timestamp);
        } else if (msg instanceof CommitQueryResponse) {
            CommitQueryResponse r = (CommitQueryResponse) msg;
            if (r.commitTimestamp != 0) {
                committed.commit(r.queryTimestamp, r.commitTimestamp);
            } else if (r.committed) {
                committed.commit(r.queryTimestamp, largestDeletedTimestamp);
            }
            List<CommitQueryCallback> cbs = null;
            synchronized (isCommittedCallbacks) {
                cbs = isCommittedCallbacks.remove(r.startTimestamp);
            }
            if (cbs == null) {
                LOG.error("Received a commit query response for a nonexisting request");
                return;
            }
            for (CommitQueryCallback cb : cbs) {
                cb.complete(r.committed);
            }
        } else if (msg instanceof CommittedTransactionReport) {
            CommittedTransactionReport ctr = (CommittedTransactionReport) msg;
            committed.commit(ctr.startTimestamp, ctr.commitTimestamp);
        } else if (msg instanceof CleanedTransactionReport) {
            CleanedTransactionReport r = (CleanedTransactionReport) msg;
            aborted.remove(r.startTimestamp);
        } else if (msg instanceof AbortedTransactionReport) {
            AbortedTransactionReport r = (AbortedTransactionReport) msg;
            aborted.add(r.startTimestamp);
        } else if (msg instanceof LargestDeletedTimestampReport) {
            LargestDeletedTimestampReport r = (LargestDeletedTimestampReport) msg;
            largestDeletedTimestamp = r.largestDeletedTimestamp;
            committed.raiseLargestDeletedTransaction(r.largestDeletedTimestamp);
        } else if (msg instanceof ZipperState) {
            // ignore
        } else {
            LOG.error("Unknown message received " + msg);
        }
        processMessage((TSOMessage) msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        LOG.error("Unexpected exception", e.getCause());

        synchronized (state) {

            if (state == State.CONNECTING) {
                state = State.RETRY_CONNECT_WAIT;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Retrying connect in " + retryDelayMs + "ms " + retries);
                }
                try {
                    retryTimer.schedule(new TimerTask() {
                        public void run() {
                            synchronized (state) {
                                state = State.DISCONNECTED;
                                try {
                                    connectIfNeeded();
                                } catch (IOException e) {
                                    bailout(e);
                                }
                            }
                        }
                    }, retryDelayMs);
                } catch (Exception cause) {
                    bailout(cause);
                }
            } else {
                LOG.error("Exception on channel", e.getCause());
            }
        }
    }

    private void bailout(Exception cause) {
        synchronized (state) {
            state = State.DISCONNECTED;
        }
        LOG.error("Unrecoverable error in client, bailing out", cause);
        Exception e = new IOException("Unrecoverable error", cause);
        Op o = queuedOps.poll();
        while (o != null) {
            o.error(e);
            o = queuedOps.poll();
        }
        synchronized (createCallbacks) {
            for (CreateCallback cb : createCallbacks) {
                cb.error(e);
            }
            createCallbacks.clear();
        }

        synchronized (commitCallbacks) {
            for (CommitCallback cb : commitCallbacks.values()) {
                cb.error(e);
            }
            commitCallbacks.clear();
        }

        synchronized (isCommittedCallbacks) {
            for (List<CommitQueryCallback> cbs : isCommittedCallbacks.values()) {
                for (CommitQueryCallback cb : cbs) {
                    cb.error(e);
                }
            }
            isCommittedCallbacks.clear();
        }
    }

    protected void processMessage(TSOMessage msg) {
    }

}
