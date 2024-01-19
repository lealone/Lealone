/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.util.Map;

import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.db.PluginManager;
import com.lealone.net.bio.BioNetFactory;
import com.lealone.net.nio.NioNetFactory;

public class NetFactoryManager extends PluginManager<NetFactory> {

    private static final NetFactoryManager instance = new NetFactoryManager();

    public static NetFactoryManager getInstance() {
        return instance;
    }

    private NetFactoryManager() {
        super(NetFactory.class);
    }

    public static NetFactory getFactory(String name) {
        NetFactory factory;
        if (NioNetFactory.NAME.equalsIgnoreCase(name))
            factory = NioNetFactory.INSTANCE;
        else if (BioNetFactory.NAME.equalsIgnoreCase(name))
            factory = BioNetFactory.INSTANCE;
        else
            factory = instance.getPlugin(name);
        return factory;
    }

    public static NetFactory getFactory(Map<String, String> config) {
        String netFactoryName = MapUtils.getString(config, ConnectionSetting.NET_FACTORY_NAME.name(),
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
