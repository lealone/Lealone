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
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.ObjectSizeEstimator;

import com.codefollower.lealone.omid.tso.persistence.BookKeeperStateBuilder;
import com.codefollower.lealone.omid.tso.persistence.LoggerProtocol;
import com.codefollower.lealone.omid.tso.persistence.LoggerAsyncCallback.AddRecordCallback;

/**
 * TSO Server with serialization
 */
public class TSOServer implements Runnable {
    private static final Log LOG = LogFactory.getLog(TSOServer.class);

    public static void main(String[] args) throws Exception {
        new TSOServer(TSOServerConfig.parseConfig(args)).run();
    }

    private final TSOServerConfig config;
    private final Object lock = new Object();

    private TSOState state;
    private boolean finish = false;

    public TSOServer(TSOServerConfig config) {
        this.config = config;
    }

    public TSOState getState() {
        return state;
    }

    @Override
    public void run() {
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

        state = BookKeeperStateBuilder.getState(config);
        if (state == null) {
            LOG.error("Couldn't build state");
            return;
        }

        state.addRecord(new byte[] { LoggerProtocol.LOG_START }, new AddRecordCallback() {
            @Override
            public void addRecordComplete(int rc, Object ctx) {
            }
        }, null);

        LOG.info("PARAM MAX_ITEMS: " + TSOState.MAX_ITEMS);
        LOG.info("PARAM BATCH_SIZE: " + config.getBatchSize());
        LOG.info("PARAM MAX_THREADS: " + maxThreads);

        final TSOHandler handler = new TSOHandler(channelGroup, state, config.getBatchSize());
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

        // Create the monitor
        ThroughputMonitor monitor = new ThroughputMonitor(state);
        // Add the parent channel to the group
        Channel channel = bootstrap.bind(new InetSocketAddress(config.getPort()));
        channelGroup.add(channel);

        // Compacter handler
        ChannelFactory comFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(), (Runtime.getRuntime().availableProcessors() * 2 + 1) * 2);
        ServerBootstrap comBootstrap = new ServerBootstrap(comFactory);
        ChannelGroup comGroup = new DefaultChannelGroup("compacter");
        final CompacterHandler comHandler = new CompacterHandler(comGroup, state);
        comBootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();
                pipeline.addLast("decoder", new ObjectDecoder());
                pipeline.addLast("encoder", new ObjectEncoder());
                pipeline.addLast("handler", comHandler);
                return pipeline;
            }
        });
        comBootstrap.setOption("tcpNoDelay", false);
        comBootstrap.setOption("child.tcpNoDelay", false);
        comBootstrap.setOption("child.keepAlive", true);
        comBootstrap.setOption("child.reuseAddress", true);
        comBootstrap.setOption("child.connectTimeoutMillis", 100);
        comBootstrap.setOption("readWriteFair", true);
        channel = comBootstrap.bind(new InetSocketAddress(config.getPort() + 1));

        // Starts the monitor
        monitor.start();
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
        comHandler.stop();
        state.stop();

        // *** Start the Netty shutdown ***

        // End the monitor
        LOG.info("End of monitor");
        monitor.interrupt();
        // Now close all channels
        LOG.info("End of channel group");
        channelGroup.close().awaitUninterruptibly();
        comGroup.close().awaitUninterruptibly();
        // Close the executor for Pipeline
        LOG.info("End of pipeline executor");
        pipelineExecutor.shutdownNow();
        // Now release resources
        LOG.info("End of resources");
        factory.releaseExternalResources();
        comFactory.releaseExternalResources();
    }

    public void stop() {
        finish = true;
        synchronized (lock) {
            lock.notify();
        }
    }
}
