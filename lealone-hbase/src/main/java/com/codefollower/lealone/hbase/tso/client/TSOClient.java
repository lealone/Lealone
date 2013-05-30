package com.codefollower.lealone.hbase.tso.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
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

import com.codefollower.lealone.hbase.transaction.Committed;
import com.codefollower.lealone.hbase.transaction.HBaseTransactionStatusTable;
import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;
import com.codefollower.lealone.hbase.tso.serialization.TSODecoder;
import com.codefollower.lealone.hbase.tso.serialization.TSOEncoder;

public class TSOClient extends SimpleChannelHandler {
    private static final Log LOG = LogFactory.getLog(TSOClient.class);

    public enum Result {
        OK, ABORTED
    };

    private enum State {
        DISCONNECTED, CONNECTING, CONNECTED, RETRY_CONNECT_WAIT
    };

    private Queue<CreateCallback> createCallbacks;
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

    public TSOClient(Configuration conf) throws IOException {
        state = State.DISCONNECTED;
        queuedOps = new ArrayBlockingQueue<Op>(200);
        retryTimer = new Timer(true);

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

    @Override
    synchronized public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) {
        e.getChannel().getPipeline().addFirst("decoder", new TSODecoder());
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

            createCallbacks.clear();
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

        commitTimestamp = HBaseTransactionStatusTable.getInstance().query(queryTimestamp);
        if (commitTimestamp != -1) {
            committed.commit(queryTimestamp, commitTimestamp);
            return true;
        } else {
            committed.commit(queryTimestamp, -2);
            return false;
        }
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
        if (msg instanceof TimestampResponse) {
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
        } else {
            LOG.error("Unknown message received " + msg);
        }
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

    }
}
