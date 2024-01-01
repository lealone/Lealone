/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import com.lealone.db.Plugin;
import com.lealone.db.PluginBase;

public abstract class ServiceExecutorFactoryBase extends PluginBase implements ServiceExecutorFactory {

    public ServiceExecutorFactoryBase(String name) {
        super(name);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return ServiceExecutorFactory.class;
    }
}
