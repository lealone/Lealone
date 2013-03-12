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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;

import com.codefollower.lealone.omid.tso.messages.MinimumTimestamp;

/**
 * ChannelHandler for the TSO Server
 *
 */
public class CompacterHandler extends SimpleChannelHandler {

    private final ChannelGroup channelGroup;
    private final ScheduledExecutorService executor;

    public CompacterHandler(ChannelGroup channelGroup, TSOState state) {
        this.channelGroup = channelGroup;
        this.executor = Executors.newScheduledThreadPool(4);
        this.executor.scheduleWithFixedDelay(new Notifier(channelGroup, state), 1, 1, TimeUnit.SECONDS);
    }

    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        System.out.println("New connection");
        channelGroup.add(ctx.getChannel());
    }

    private static class Notifier implements Runnable {
        private final ChannelGroup channelGroup;
        private final TSOState sharedState;

        public Notifier(ChannelGroup channelGroup, TSOState sharedState) {
            this.channelGroup = channelGroup;
            this.sharedState = sharedState;
        }

        @Override
        public void run() {
            long timestamp = sharedState.uncommited.getFirstUncommitted();
            channelGroup.write(new MinimumTimestamp(timestamp));
        }
    }

    public void stop() {
        this.executor.shutdown();
    }
}
