/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.util.Map;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.nio.NioNetFactory;

public interface NetFactory extends Plugin {

    public static final String NIO = "nio";
    public static final String BIO = "bio";

    NetServer createNetServer();

    NetClient createNetClient();

    default NetEventLoop createNetEventLoop(Scheduler scheduler, long loopInterval) {
        return null;
    }

    default boolean isBio() {
        return false;
    }

    public static NetFactory getFactory(String name) {
        NetFactory factory;
        if (NIO.equalsIgnoreCase(name))
            factory = NioNetFactory.NIO;
        else if (BIO.equalsIgnoreCase(name))
            factory = NioNetFactory.BIO;
        else
            factory = PluginManager.getPlugin(NetFactory.class, name);
        return factory;
    }

    public static NetFactory getFactory(Map<String, String> config) {
        return getFactory(config, Constants.DEFAULT_NET_FACTORY_NAME);
    }

    public static NetFactory getFactory(Map<String, String> config, String defaultFactoryName) {
        String netFactoryName = MapUtils.getString(config, ConnectionSetting.NET_FACTORY_NAME.name(),
                defaultFactoryName);
        NetFactory factory = getFactory(netFactoryName);
        if (factory == null) {
            throw new ConfigException("NetFactory '" + netFactoryName + "' can not found");
        }
        factory.init(config);
        return factory;
    }
}
