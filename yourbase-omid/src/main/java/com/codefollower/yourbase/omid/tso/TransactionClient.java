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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * Simple Transaction Client using Serialization
 * @author maysam
 *
 */
public class TransactionClient {

    /**
     * Main class for Client taking from two to four arguments<br>
     * -host for server<br>
     * -port for server<br>
     * -number of message (default is 256)<br>
     * -MAX_IN_FLIGHT
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // Print usage if no argument is specified.
        if (args.length < 2 || args.length > 7) {
            System.err.println("Usage: " + TransactionClient.class.getSimpleName()
                    + " <host> <port> [<number of messages>] [<MAX_IN_FLIGHT>] [<connections>] [<pause>] [<% reads>]");
            return;
        }

        // Parse options.
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int nbMessage;

        if (args.length >= 3) {
            nbMessage = Integer.parseInt(args[2]);
        } else {
            nbMessage = 256;
        }
        int inflight = 10;
        if (args.length >= 4) {
            inflight = Integer.parseInt(args[3]);
        }

        int runs = 1;
        if (args.length >= 5) {
            runs = Integer.parseInt(args[4]);
        }

        boolean pauseClient = false;
        if (args.length >= 6) {
            pauseClient = Boolean.parseBoolean(args[5]);
        }

        float percentRead = 0;
        if (args.length >= 7) {
            percentRead = Float.parseFloat(args[6]);
        }

        // *** Start the Netty configuration ***

        // Start client with Nb of active threads = 3 as maximum.
        ChannelFactory factory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool(), 30);

        // Create the global ChannelGroup
        ChannelGroup channelGroup = new DefaultChannelGroup(TransactionClient.class.getName());

        List<ClientHandler> handlers = new ArrayList<ClientHandler>();

        Configuration conf = HBaseConfiguration.create();
        conf.set("tso.host", host);
        conf.setInt("tso.port", port);
        conf.setInt("tso.executor.threads", 10);

        for (int i = 0; i < runs; ++i) {
            // Create the associated Handler
            ClientHandler handler = new ClientHandler(conf, nbMessage, inflight, pauseClient, percentRead);

            // *** Start the Netty running ***

            System.out.println("PARAM MAX_ROW: " + ClientHandler.MAX_ROW);
            System.out.println("PARAM DB_SIZE: " + ClientHandler.DB_SIZE);
            System.out.println("pause " + pauseClient);
            System.out.println("readPercent " + percentRead);

            handlers.add(handler);

            if ((i - 1) % 20 == 0)
                Thread.sleep(1000);
        }

        // Wait for the Traffic to finish
        for (ClientHandler handler : handlers) {
            boolean result = handler.waitForAll();
            System.out.println("Result: " + result);
        }

        // *** Start the Netty shutdown ***

        // Now close all channels
        System.out.println("close channelGroup");
        channelGroup.close().awaitUninterruptibly();
        // Now release resources
        System.out.println("close external resources");
        factory.releaseExternalResources();
    }
}
