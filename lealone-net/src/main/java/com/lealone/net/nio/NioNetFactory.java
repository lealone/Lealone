/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.db.Constants;
import com.lealone.net.NetClient;
import com.lealone.net.NetFactoryBase;
import com.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = Constants.DEFAULT_NET_FACTORY_NAME;
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
        return new NioServerAccepter();
    }

    @Override
    public NioEventLoop createNetEventLoop(long loopInterval) {
        return new NioEventLoop(config, loopInterval);
    }
}
