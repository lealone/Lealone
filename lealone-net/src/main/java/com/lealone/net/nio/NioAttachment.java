/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.db.DataBuffer;
import com.lealone.net.AsyncConnection;

public class NioAttachment {
    public AsyncConnection conn;
    DataBuffer dataBuffer;
    int endOfStreamCount;
    int state = 0;
}