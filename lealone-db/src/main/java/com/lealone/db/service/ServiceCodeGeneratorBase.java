/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;

public abstract class ServiceCodeGeneratorBase extends PluginBase implements ServiceCodeGenerator {

    public ServiceCodeGeneratorBase(String name) {
        super(name);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return ServiceCodeGenerator.class;
    }
}
