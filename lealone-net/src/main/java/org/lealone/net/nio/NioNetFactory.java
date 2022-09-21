/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Constants;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = Constants.DEFAULT_NET_FACTORY_NAME;
    public static final NioNetFactory INSTANCE = new NioNetFactory();

    public NioNetFactory() {
        super(NAME, NioEventLoopClient.getInstance());
    }

    @Override
    public NetServer createNetServer() {
        return new ServerAccepter();
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
