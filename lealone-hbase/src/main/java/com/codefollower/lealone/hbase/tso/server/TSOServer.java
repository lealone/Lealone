package com.codefollower.lealone.hbase.tso.server;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.AdaptiveReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ObjectSizeEstimator;

/**
 * TSO Server with serialization
 */
public class TSOServer {
    private static final Log LOG = LogFactory.getLog(TSOServer.class);

    public static void main(String[] args) throws Exception {
        int port = Integer.valueOf(args[0]);
        new TSOServer().run(port);
    }

    private final Object lock = new Object();
    private boolean finish = false;

    public void run(int port) {
        // *** Start the Netty configuration ***
        // Start server with Nb of active threads = 2*NB CPU + 1 as maximum.
        ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);

        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TSOServer.class.getName());
        // threads max
        // int maxThreads = Runtime.getRuntime().availableProcessors() *2 + 1;
        int maxThreads = 5;
        // Memory limitation: 1MB by channel, 1GB global, 100 ms of timeout
        ThreadPoolExecutor pipelineExecutor = new OrderedMemoryAwareThreadPoolExecutor(maxThreads, 1048576, 1073741824, 100,
                TimeUnit.MILLISECONDS, new ObjectSizeEstimator() {
                    @Override
                    public int estimateSize(Object o) {
                        return 1000;
                    }
                }, Executors.defaultThreadFactory());

        LOG.info("PARAM MAX_THREADS: " + maxThreads);

        final TSOHandler handler = new TSOHandler(channelGroup);
        handler.start();

        bootstrap.setPipelineFactory(new TSOPipelineFactory(pipelineExecutor, handler));
        bootstrap.setOption("tcpNoDelay", false);
        //setting buffer size can improve I/O
        bootstrap.setOption("child.sendBufferSize", 1048576);
        bootstrap.setOption("child.receiveBufferSize", 1048576);
        // better to have an receive buffer predictor
        bootstrap.setOption("receiveBufferSizePredictorFactory", new AdaptiveReceiveBufferSizePredictorFactory());
        //if the server is sending 1000 messages per sec, optimum write buffer water marks will
        //prevent unnecessary throttling, Check NioSocketChannelConfig doc
        bootstrap.setOption("writeBufferLowWaterMark", 32 * 1024);
        bootstrap.setOption("writeBufferHighWaterMark", 64 * 1024);

        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setOption("child.reuseAddress", true);
        bootstrap.setOption("child.connectTimeoutMillis", 60000);

        // *** Start the Netty running ***

        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(port));
        channelGroup.add(channel);

        synchronized (lock) {
            while (!finish) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        handler.stop();

        // *** Start the Netty shutdown ***

        // Now close all channels
        LOG.info("End of channel group");
        channelGroup.close().awaitUninterruptibly();
        // Close the executor for Pipeline
        LOG.info("End of pipeline executor");
        pipelineExecutor.shutdownNow();
        // Now release resources
        LOG.info("End of resources");
        factory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
        synchronized (lock) {
            lock.notify();
        }
    }
}
