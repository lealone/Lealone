/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionSetting;
import org.lealone.db.Constants;
import org.lealone.net.NetClient;
import org.lealone.net.NetFactoryBase;
import org.lealone.net.NetServer;

public class NioNetFactory extends NetFactoryBase {

    public static final String NAME = Constants.DEFAULT_NET_FACTORY_NAME;
    public static final NioNetFactory INSTANCE = new NioNetFactory();

    private final AtomicInteger index = new AtomicInteger(0);
    private NioEventLoopClient[] netClients;

    public NioNetFactory() {
        super(NAME);
    }

    @Override
    public void init(Map<String, String> config) {
        init(config, false);
    }

    @Override
    public void init(Map<String, String> config, boolean initClient) {
        super.init(config);
        if (initClient && netClients == null) {
            synchronized (this) {
                if (netClients == null) {
                    int count = MapUtils.getInt(config, ConnectionSetting.NET_CLIENT_COUNT.name(),
                            Runtime.getRuntime().availableProcessors());
                    NioEventLoopClient[] netClients = new NioEventLoopClient[count];
                    for (int i = 0; i < count; i++)
                        netClients[i] = new NioEventLoopClient(i);
                    this.netClients = netClients;
                }
            }
        }
    }

    @Override
    public NetClient getNetClient() {
        return netClients[index.getAndIncrement() % netClients.length];
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
