/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;

public class NioAttachment {
    public AsyncConnection conn;
    NetBuffer inBuffer;
    int state = 0;
}