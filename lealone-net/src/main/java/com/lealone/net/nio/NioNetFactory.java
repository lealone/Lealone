/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetClient;
import com.lealone.net.NetFactory;
import com.lealone.net.NetFactoryBase;
import com.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final NioNetFactory NIO = new NioNetFactory(false);
    public static final NioNetFactory BIO = new NioNetFactory(true);

    private final boolean block;

    public NioNetFactory() {
        this(false);
    }

    public NioNetFactory(boolean block) {
        super(NetFactory.NIO);
        this.block = block;
    }

    @Override
    public NetClient createNetClient() {
        return new NioNetClient(block);
    }

    @Override
    public NetServer createNetServer() {
        return new NioNetServer();
    }

    @Override
    public NioEventLoop createNetEventLoop(Scheduler scheduler, long loopInterval) {
        return new NioEventLoop(scheduler, loopInterval, config);
    }

    @Override
    public boolean isBio() {
        return block;
    }
}
