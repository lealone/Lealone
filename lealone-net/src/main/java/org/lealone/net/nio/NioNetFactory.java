/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.net.AsyncConnectionAccepter;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = Constants.DEFAULT_NET_FACTORY_NAME;

    public NioNetFactory() {
        super(NAME, NioNetClient.getInstance());
    }

    @Override
    public NetServer createNetServer() {
        // 如果在调度器里负责网络IO，只需要启动接收器即可
        if (NetEventLoop.isRunInScheduler(config))
            return new AsyncConnectionAccepter();
        else
            return new NioNetServer();
    }

    @Override
    public NioEventLoop createNetEventLoop(String loopIntervalKey, long defaultLoopInterval) {
        try {
            return new NioEventLoop(config, loopIntervalKey, defaultLoopInterval);
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }
}
