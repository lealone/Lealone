/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import org.lealone.db.DataBuffer;
import org.lealone.net.AsyncConnection;

class NioAttachment {
    AsyncConnection conn;
    DataBuffer dataBuffer;
    int endOfStreamCount;
    int state = 0;
}