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
package com.codefollower.lealone.omid.regionserver;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;

import com.codefollower.lealone.omid.client.ColumnWrapper;
import com.codefollower.lealone.omid.tso.messages.MinimumTimestamp;

public class Compacter extends BaseRegionObserver {

    private static ExecutorService bossExecutor = Executors.newCachedThreadPool();
    private static ExecutorService workerExecutor = Executors.newCachedThreadPool();
    private volatile long minTimestamp;
    private ClientBootstrap bootstrap;
    private ChannelFactory factory;
    private Channel channel;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        System.out.println("Starting compacter");
        Configuration conf = e.getConfiguration();
        factory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor, 3);
        bootstrap = new ClientBootstrap(factory);

        bootstrap.getPipeline().addLast("decoder", new ObjectDecoder());
        bootstrap.getPipeline().addLast("handler", new Handler());
        bootstrap.setOption("tcpNoDelay", false);
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("reuseAddress", true);
        bootstrap.setOption("connectTimeoutMillis", 100);

        String host = conf.get("tso.host");
        int port = conf.getInt("tso.port", 1234) + 1;

        if (host == null) {
            throw new IOException("tso.host missing from configuration");
        }

        bootstrap.connect(new InetSocketAddress(host, port)).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    System.out.println("Compacter connected!");
                    channel = future.getChannel();
                } else {
                    System.out.println("Connection failed");
                }
            }
        });
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        System.out.println("Stoping compacter");
        if (channel != null) {
            System.out.println("Calling close");
            channel.close();
        }
        System.out.println("Compacter stopped");
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) {
        if (e.getEnvironment().getRegion().getRegionInfo().isMetaTable()) {
            return scanner;
        } else {
            return new CompacterScanner(scanner, minTimestamp);
        }
    }

    private static class CompacterScanner implements InternalScanner {
        private InternalScanner internalScanner;
        private long minTimestamp;
        private Set<ColumnWrapper> columnsSeen = new HashSet<ColumnWrapper>();
        private byte[] lastRowId = null;

        public CompacterScanner(InternalScanner internalScanner, long minTimestamp) {
            this.minTimestamp = minTimestamp;
            this.internalScanner = internalScanner;
            System.out.println("Created scanner with " + minTimestamp);
        }

        @Override
        public boolean next(List<KeyValue> results) throws IOException {
            return next(results, -1);
        }

        @Override
        public boolean next(List<KeyValue> result, int limit) throws IOException {
            boolean moreRows = false;
            List<KeyValue> raw = new ArrayList<KeyValue>(limit);
            while (limit == -1 || result.size() < limit) {
                int toReceive = limit == -1 ? -1 : limit - result.size();
                moreRows = internalScanner.next(raw, toReceive);
                if (raw.size() > 0) {
                    byte[] currentRowId = raw.get(0).getRow();
                    if (!Arrays.equals(currentRowId, lastRowId)) {
                        columnsSeen.clear();
                        lastRowId = currentRowId;
                    }
                }
                for (KeyValue kv : raw) {
                    ColumnWrapper column = new ColumnWrapper(kv.getFamily(), kv.getQualifier());
                    if (columnsSeen.add(column) || kv.getTimestamp() > minTimestamp) {
                        result.add(kv);
                    } else {
                        System.out.println("Discarded " + kv);
                    }
                }
                if (raw.size() < toReceive || toReceive == -1) {
                    columnsSeen.clear();
                    break;
                }
                raw.clear();
            }
            if (!moreRows) {
                columnsSeen.clear();
            }
            return moreRows;
        }

        @Override
        public void close() throws IOException {
            internalScanner.close();
        }

        @Override
        public boolean next(List<KeyValue> results, String metric) throws IOException {
            return next(results);
        }

        @Override
        public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
            return next(result, limit);
        }

    }

    private class Handler extends SimpleChannelUpstreamHandler {
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
            Object message = e.getMessage();
            //         System.out.println("Received " + message);
            if (message instanceof MinimumTimestamp) {
                Compacter.this.minTimestamp = ((MinimumTimestamp) message).getTimestamp();
            } else {
                System.out.println("Wtf " + message);
            }
        }
    }
}
