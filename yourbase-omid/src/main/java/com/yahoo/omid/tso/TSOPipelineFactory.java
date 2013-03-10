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

package com.yahoo.omid.tso;

import java.util.concurrent.Executor;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.util.Timer;

import com.yahoo.omid.tso.serialization.TSODecoder;
import com.yahoo.omid.tso.serialization.TSOEncoder;

/**
 * @author maysam
 *
 */
public class TSOPipelineFactory implements ChannelPipelineFactory {

    private Executor pipelineExecutor = null;

    /**
     * Constructor
     * @param channelGroup
     * @param pipelineExecutor
     * @param answer
     * @param to The shared timestamp oracle
     * @param shared The shared state among handlers
     */
    public TSOPipelineFactory(Executor pipelineExecutor, ChannelHandler handler) {
        super();
        this.pipelineExecutor = pipelineExecutor;
        this.handler = handler;
    }

    /**
     * Initiate the Pipeline for the newly active connection with ObjectXxcoder.
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    //TSODecoder d = new TSODecoder();
    //TSOEncoder e = new TSOEncoder();
    ExecutionHandler x = null;// = new ExecutionHandler(pipelineExecutor);
    ChannelHandler handler = null;
    Timer timer = null;
    public static BandwidthMeterHandler bwhandler = null;

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast("decoder", new TSODecoder(null));
        pipeline.addLast("encoder", new TSOEncoder());
        synchronized (this) {
            if (x == null)
                x = new ExecutionHandler(pipelineExecutor);
            if (bwhandler == null)
                bwhandler = new BandwidthMeterHandler();
            //          if (timer == null)
            //             timer = new HashedWheelTimer();
        }
        pipeline.addLast("pipelineExecutor", x);
        //      pipeline.addLast("timeout", new WriteTimeoutHandler(timer, 10));
        pipeline.addFirst("bw", bwhandler);

        pipeline.addLast("handler", handler);
        return pipeline;
    }

}
