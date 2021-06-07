/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import org.lealone.db.PluggableEngineManager;

public class ProtocolServerEngineManager extends PluggableEngineManager<ProtocolServerEngine> {

    private static final ProtocolServerEngineManager instance = new ProtocolServerEngineManager();

    public static ProtocolServerEngineManager getInstance() {
        return instance;
    }

    private ProtocolServerEngineManager() {
        super(ProtocolServerEngine.class);
    }

}
