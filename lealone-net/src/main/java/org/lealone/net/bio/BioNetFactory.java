/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.bio;

import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.net.NetClient;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class BioNetFactory extends NetFactoryBase {

    public static final String NAME = "bio";
    public static final BioNetFactory INSTANCE = new BioNetFactory();

    private BioNetClient netClient = new BioNetClient();

    public BioNetFactory() {
        super(NAME);
    }

    @Override
    public void init(Map<String, String> config, boolean initClient) {
        super.init(config);
    }

    @Override
    public NetClient getNetClient() {
        return netClient;
    }

    @Override
    public NetServer createNetServer() {
        throw DbException.getInternalError();
    }

    @Override
    public NetEventLoop createNetEventLoop(String loopIntervalKey, long defaultLoopInterval) {
        throw DbException.getInternalError();
    }
}
