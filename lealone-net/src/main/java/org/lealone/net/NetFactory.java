/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.db.Plugin;

public interface NetFactory extends Plugin {

    NetServer createNetServer();

    NetClient createNetClient();

    default NetEventLoop createNetEventLoop(long loopInterval, boolean isThreadSafe) {
        return null;
    }
}
