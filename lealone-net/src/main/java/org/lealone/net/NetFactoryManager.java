/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.db.PluggableEngineManager;

public class NetFactoryManager extends PluggableEngineManager<NetFactory> {

    private static final NetFactoryManager instance = new NetFactoryManager();

    public static NetFactoryManager getInstance() {
        return instance;
    }

    private NetFactoryManager() {
        super(NetFactory.class);
    }

    public static NetFactory getFactory(String name) {
        return instance.getEngine(name);
    }

    public static NetFactory getFactory(Map<String, String> config) {
        String netFactoryName = config.get(Constants.NET_FACTORY_NAME_KEY);
        if (netFactoryName == null)
            netFactoryName = Constants.DEFAULT_NET_FACTORY_NAME;

        NetFactory factory = getFactory(netFactoryName);
        if (factory == null) {
            throw new RuntimeException("NetFactory '" + netFactoryName + "' can not found");
        }
        return factory;
    }

    public static void registerFactory(NetFactory factory) {
        instance.registerEngine(factory);
    }

    public static void deregisterFactory(NetFactory factory) {
        instance.deregisterEngine(factory);
    }
}
