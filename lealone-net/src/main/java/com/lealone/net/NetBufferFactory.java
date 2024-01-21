/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;

public class NetBufferFactory {

    private static final NetBufferFactory instance = new NetBufferFactory();

    public static NetBufferFactory getInstance() {
        return instance;
    }

    public NetBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory) {
        return new NetBuffer(dataBufferFactory.create(initialSizeHint));
    }

    public NetBuffer createBuffer(DataBuffer dataBuffer) {
        return new NetBuffer(dataBuffer);
    }
}
