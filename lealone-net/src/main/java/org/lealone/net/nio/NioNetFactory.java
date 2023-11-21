/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import org.lealone.db.Constants;
import org.lealone.net.NetClient;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = Constants.DEFAULT_NET_FACTORY_NAME;
    public static final NioNetFactory INSTANCE = new NioNetFactory();

    public NioNetFactory() {
        super(NAME);
    }

    @Override
    public NetClient createNetClient() {
        return new NioClient();
    }

    @Override
    public NetServer createNetServer() {
        return new NioServerAccepter();
    }

    @Override
    public NioEventLoop createNetEventLoop(long loopInterval, boolean isThreadSafe) {
        return new NioEventLoop(config, loopInterval, isThreadSafe);
    }
}
