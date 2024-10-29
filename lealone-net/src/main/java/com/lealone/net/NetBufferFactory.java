/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;

public class NetBufferFactory {

    public static final NetBufferFactory INSTANCE = new NetBufferFactory();

    public NetBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory) {
        return new NetBuffer(dataBufferFactory.create(initialSizeHint));
    }

    // 外部插件会用到
    public NetBuffer createBuffer(DataBuffer dataBuffer) {
        return new NetBuffer(dataBuffer);
    }
}
