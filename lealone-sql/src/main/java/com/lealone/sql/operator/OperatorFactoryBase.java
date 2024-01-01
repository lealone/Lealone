/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.operator;

import com.lealone.db.Plugin;
import com.lealone.db.PluginBase;

public abstract class OperatorFactoryBase extends PluginBase implements OperatorFactory {

    public OperatorFactoryBase(String name) {
        super(name);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return OperatorFactory.class;
    }
}
