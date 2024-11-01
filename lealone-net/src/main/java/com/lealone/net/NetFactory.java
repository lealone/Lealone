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
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.bio.BioNetFactory;
import com.lealone.net.nio.NioNetFactory;

public interface NetFactory extends Plugin {

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
        if (NioNetFactory.NAME.equalsIgnoreCase(name))
            factory = NioNetFactory.INSTANCE;
        else if (BioNetFactory.NAME.equalsIgnoreCase(name))
            factory = BioNetFactory.INSTANCE;
        else
            factory = PluginManager.getPlugin(NetFactory.class, name);
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
}
