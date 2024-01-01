/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;
import com.lealone.net.NetBufferFactory;

public class NioBufferFactory implements NetBufferFactory {

    private static final NioBufferFactory instance = new NioBufferFactory();

    public static NioBufferFactory getInstance() {
        return instance;
    }

    private NioBufferFactory() {
    }

    @Override
    public NioBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory) {
        return new NioBuffer(dataBufferFactory.create(initialSizeHint));
    }

    @Override
    public NioBuffer createBuffer(DataBuffer dataBuffer) {
        return new NioBuffer(dataBuffer);
    }
}
