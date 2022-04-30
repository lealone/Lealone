/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Map;

import org.lealone.common.util.MapUtils;
import org.lealone.db.Constants;
import org.lealone.db.PluginManager;

public class NetFactoryManager extends PluginManager<NetFactory> {

    private static final NetFactoryManager instance = new NetFactoryManager();

    public static NetFactoryManager getInstance() {
        return instance;
    }

    private NetFactoryManager() {
        super(NetFactory.class);
    }

    public static NetFactory getFactory(String name) {
        return instance.getPlugin(name);
    }

    public static NetFactory getFactory(Map<String, String> config) {
        String netFactoryName = MapUtils.getString(config, Constants.NET_FACTORY_NAME_KEY,
                Constants.DEFAULT_NET_FACTORY_NAME);
        NetFactory factory = getFactory(netFactoryName);
        if (factory == null) {
            throw new RuntimeException("NetFactory '" + netFactoryName + "' can not found");
        }
        factory.init(config);
        return factory;
    }

    public static void registerFactory(NetFactory factory) {
        instance.registerPlugin(factory);
    }

    public static void deregisterFactory(NetFactory factory) {
        instance.deregisterPlugin(factory);
    }
}
