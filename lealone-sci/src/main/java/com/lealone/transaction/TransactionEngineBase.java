/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;

public abstract class TransactionEngineBase extends PluginBase implements TransactionEngine {

    public TransactionEngineBase(String name) {
        super(name);
    }

    @Override
    public boolean supportsMVCC() {
        return false;
    }

    @Override
    public void checkpoint() {
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return TransactionEngine.class;
    }
}
