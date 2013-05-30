/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefollower.lealone.hbase.tso.server;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;

import com.codefollower.lealone.hbase.tso.serialization.TSODecoder;
import com.codefollower.lealone.hbase.tso.serialization.TSOEncoder;

/**
 * @author maysam
 *
 */
public class TSOPipelineFactory implements ChannelPipelineFactory {

    private Executor pipelineExecutor = null;
    private ExecutionHandler x = null;// = new ExecutionHandler(pipelineExecutor);
    private ChannelHandler handler = null;

    /**
     * Constructor
     * @param channelGroup
     * @param pipelineExecutor
     * @param answer
     * @param to The shared timestamp oracle
     * @param shared The shared state among handlers
     */
    public TSOPipelineFactory(Executor pipelineExecutor, ChannelHandler handler) {
        this.pipelineExecutor = pipelineExecutor;
        this.handler = handler;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("decoder", new TSODecoder());
        pipeline.addLast("encoder", new TSOEncoder());
        synchronized (this) {
            if (x == null)
                x = new ExecutionHandler(pipelineExecutor);
            //          if (timer == null)
            //             timer = new HashedWheelTimer();
        }
        pipeline.addLast("pipelineExecutor", x);
        //      pipeline.addLast("timeout", new WriteTimeoutHandler(timer, 10));

        pipeline.addLast("handler", handler);
        return pipeline;
    }

}
