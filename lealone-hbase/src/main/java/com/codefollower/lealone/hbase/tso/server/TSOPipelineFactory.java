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
