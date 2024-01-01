/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.lealone.db.PluggableEngine;

public interface ProtocolServerEngine extends PluggableEngine {

    public static final List<ProtocolServer> startedServers = new CopyOnWriteArrayList<>();

    ProtocolServer getProtocolServer();

}
