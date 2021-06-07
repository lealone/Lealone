/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import org.lealone.db.PluggableEngine;

public interface ProtocolServerEngine extends PluggableEngine {

    ProtocolServer getProtocolServer();

    boolean isInited();

}
