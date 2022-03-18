/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.db.PluginBase;

public abstract class ProtocolServerEngineBase extends PluginBase implements ProtocolServerEngine {

    protected boolean inited;

    public ProtocolServerEngineBase(String name) {
        super(name);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        inited = true;
    }

    @Override
    public boolean isInited() {
        return inited;
    }
}
