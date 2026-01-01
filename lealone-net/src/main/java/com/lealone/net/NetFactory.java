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

    NetClient createNetClient();

    NetServer createNetServer();

    NetEventLoop createNetEventLoop(Scheduler scheduler, long loopInterval);

    public static boolean isBio(Map<String, String> config) {
        return BIO.equalsIgnoreCase(MapUtils.getString(config, ConnectionSetting.NET_FACTORY_NAME.name(),
                Constants.DEFAULT_NET_FACTORY_NAME));
    }

    public static NetFactory getFactory(Map<String, String> config) {
        return getFactory(config, Constants.DEFAULT_NET_FACTORY_NAME);
    }

    public static NetFactory getFactory(Map<String, String> config, String defaultName) {
        String name;
        NetFactory factory;
        AsyncConnectionPool.setMaxExclusiveSize(config);
        if (AsyncConnectionPool.isExceededMaxExclusiveSize()) {
            config.put(ConnectionSetting.NET_FACTORY_NAME.name(), NIO);
            config.put(ConnectionSetting.IS_SHARED.name(), "true");
            name = NIO;
        } else {
            name = MapUtils.getString(config, ConnectionSetting.NET_FACTORY_NAME.name(), defaultName);
        }
        // nio和bio都用NioNetFactory实现
        if (NIO.equalsIgnoreCase(name) || BIO.equalsIgnoreCase(name)) {
            factory = NioNetFactory.INSTANCE;
        } else {
            factory = PluginManager.getPlugin(NetFactory.class, name);
            if (factory == null) {
                throw new ConfigException("NetFactory '" + name + "' can not found");
            }
        }
        factory.init(config);
        return factory;
    }
}
