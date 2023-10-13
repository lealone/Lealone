/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.db.PluginBase;

public abstract class NetFactoryBase extends PluginBase implements NetFactory {

    public NetFactoryBase(String name) {
        super(name);
    }
}
