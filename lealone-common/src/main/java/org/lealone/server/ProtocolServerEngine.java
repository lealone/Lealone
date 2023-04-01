/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.db.PluggableEngine;

public interface ProtocolServerEngine extends PluggableEngine {

    public static final List<ProtocolServer> startedServers = new CopyOnWriteArrayList<>();

    ProtocolServer getProtocolServer();

    boolean isInited();

}
