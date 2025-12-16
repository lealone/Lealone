/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.NetClient;
import com.lealone.net.NetFactoryBase;
import com.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = "nio";
    public static final NioNetFactory INSTANCE = new NioNetFactory();

    public NioNetFactory() {
        super(NAME);
    }

    @Override
    public NetClient createNetClient() {
        return new NioNetClient();
    }

    @Override
    public NetServer createNetServer() {
        return new NioNetServer();
    }

    @Override
    public NioEventLoop createNetEventLoop(Scheduler scheduler, long loopInterval) {
        return new NioEventLoop(scheduler, loopInterval, config);
    }
}
