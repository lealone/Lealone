/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;

public abstract class ServiceExecutorFactoryBase extends PluginBase implements ServiceExecutorFactory {

    public ServiceExecutorFactoryBase(String name) {
        super(name);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return ServiceExecutorFactory.class;
    }
}
