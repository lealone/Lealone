/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.session;

import org.lealone.db.Plugin;
import org.lealone.db.PluginBase;

public abstract class SessionFactoryBase extends PluginBase implements SessionFactory {

    public SessionFactoryBase() {
        setName(getClass().getSimpleName());
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return SessionFactory.class;
    }
}
