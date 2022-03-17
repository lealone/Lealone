/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.db.PluginBase;

public abstract class NetFactoryBase extends PluginBase implements NetFactory {

    protected final NetClient netClient;

    public NetFactoryBase(String name, NetClient netClient) {
        super(name);
        this.netClient = netClient;
    }

    @Override
    public NetClient getNetClient() {
        return netClient;
    }
}
