/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;

public abstract class SessionFactoryBase extends PluginBase implements SessionFactory {

    public SessionFactoryBase() {
        setName(getClass().getSimpleName());
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return SessionFactory.class;
    }
}
