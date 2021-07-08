/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Map;

public abstract class NetFactoryBase implements NetFactory {

    protected final String name;
    protected final NetClient netClient;
    protected Map<String, String> config;

    public NetFactoryBase(String name, NetClient netClient) {
        this.name = name;
        this.netClient = netClient;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public NetClient getNetClient() {
        return netClient;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void close() {
    }
}
