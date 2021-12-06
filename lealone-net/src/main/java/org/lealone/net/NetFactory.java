/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.db.PluggableEngine;

public interface NetFactory extends PluggableEngine {

    NetServer createNetServer();

    NetClient getNetClient();

    default NetEventLoop createNetEventLoop(String loopIntervalKey, long defaultLoopInterval) {
        return null;
    }
}
