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

package com.codefollower.yourbase.omid.tso;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.codefollower.yourbase.omid.TestUtils;
import com.codefollower.yourbase.omid.tso.ClientHandler;
import com.codefollower.yourbase.omid.tso.RowKey;
import com.codefollower.yourbase.omid.tso.TSOServer;
import com.codefollower.yourbase.omid.tso.TSOServerConfig;
import com.codefollower.yourbase.omid.tso.TSOState;
import com.codefollower.yourbase.omid.tso.TransactionClient;
import com.codefollower.yourbase.omid.tso.messages.TimestampRequest;
import com.codefollower.yourbase.omid.tso.messages.TimestampResponse;

public class TSOTestBase {
    private static final Log LOG = LogFactory.getLog(TSOTestBase.class);

    //private static Thread bkthread;
    //private static Thread tsothread;
    private static ExecutorService bkExecutor;
    private static ExecutorService tsoExecutor;

    protected static TestClientHandler clientHandler;
    protected static TestClientHandler secondClientHandler;
    private static ChannelGroup channelGroup;
    private static ChannelFactory channelFactory;
    protected static TSOState state;
    private static TSOServer tso;

    final static public RowKey r1 = new RowKey(new byte[] { 0xd, 0xe, 0xa, 0xd }, new byte[] { 0xb, 0xe, 0xe, 0xf });
    final static public RowKey r2 = new RowKey(new byte[] { 0xb, 0xa, 0xa, 0xd }, new byte[] { 0xc, 0xa, 0xf, 0xe });

    public static void setupClient() throws IOException {

        // *** Start the Netty configuration ***

        Configuration conf = HBaseConfiguration.create();
        conf.set("tso.host", "localhost");
        conf.setInt("tso.port", 1234);

        // Start client with Nb of active threads = 3 as maximum.
        channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool(), 3);
        // Create the bootstrap
        // Create the global ChannelGroup
        channelGroup = new DefaultChannelGroup(TransactionClient.class.getName());
        // Create the associated Handler
        clientHandler = new TestClientHandler(conf);

        // *** Start the Netty running ***

        System.out.println("PARAM MAX_ROW: " + ClientHandler.MAX_ROW);
        System.out.println("PARAM DB_SIZE: " + ClientHandler.DB_SIZE);

        // Connect to the server, wait for the connection and get back the channel
        clientHandler.await();

        // Second client handler
        secondClientHandler = new TestClientHandler(conf);

        // *** Start the Netty running ***

        System.out.println("PARAM MAX_ROW: " + ClientHandler.MAX_ROW);
        System.out.println("PARAM DB_SIZE: " + ClientHandler.DB_SIZE);
    }

    public static void teardownClient() {
        // Now close all channels
        System.out.println("close channelGroup");
        channelGroup.close().awaitUninterruptibly();
        // Now release resources
        System.out.println("close external resources");
        channelFactory.releaseExternalResources();
    }

    @BeforeClass
    public static void setupBookkeeper() throws Exception {
        System.out.println("PATH : " + System.getProperty("java.library.path"));

        if (bkExecutor == null) {
            bkExecutor = Executors.newSingleThreadExecutor();
            Runnable bkTask = new Runnable() {
                public void run() {
                    try {
                        Thread.currentThread().setName("BookKeeper");
                        String[] args = new String[1];
                        args[0] = "5";
                        LOG.info("Starting bk");
                        LocalBookKeeper.main(args);
                    } catch (InterruptedException e) {
                        // go away quietly
                    } catch (Exception e) {
                        LOG.error("Error starting local bk", e);
                    }
                }
            };

            bkExecutor.execute(bkTask);
        }
    }

    @AfterClass
    public static void teardownBookkeeper() throws Exception {

        if (bkExecutor != null) {
            bkExecutor.shutdownNow();
        }
        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);

        Thread.sleep(10);
    }

    @Before
    public void setupTSO() throws Exception {
        if (!LocalBookKeeper.waitForServerUp("localhost:2181", 10000)) {
            throw new Exception("Error starting zookeeper/bookkeeper");
        }

        /*
         * TODO: Fix LocalBookKeeper to wait until the bookies are up
         * instead of waiting only until the zookeeper server is up.
         */
        Thread.sleep(500);

        LOG.info("Starting TSO");
        tso = new TSOServer(TSOServerConfig.configFactory(1234, 0, recoveryEnabled(), 4, 2, new String("localhost:2181")));
        tsoExecutor = Executors.newSingleThreadExecutor();
        tsoExecutor.execute(tso);
        TestUtils.waitForSocketListening("localhost", 1234, 100);
        LOG.info("Finished loading TSO");

        state = tso.getState();

        Thread.currentThread().setName("JUnit Thread");

        setupClient();
    }

    @After
    public void teardownTSO() throws Exception {

        clientHandler.sendMessage(new TimestampRequest());
        while (!(clientHandler.receiveMessage() instanceof TimestampResponse))
            ; // Do nothing
        clientHandler.clearMessages();
        clientHandler.setAutoFullAbort(true);
        secondClientHandler.sendMessage(new TimestampRequest());
        while (!(secondClientHandler.receiveMessage() instanceof TimestampResponse))
            ; // Do nothing
        secondClientHandler.clearMessages();
        secondClientHandler.setAutoFullAbort(true);

        tso.stop();
        if (tsoExecutor != null) {
            tsoExecutor.shutdownNow();
        }
        teardownClient();

        TestUtils.waitForSocketNotListening("localhost", 1234, 1000);

        Thread.sleep(10);
    }

    protected boolean recoveryEnabled() {
        return false;
    }

}