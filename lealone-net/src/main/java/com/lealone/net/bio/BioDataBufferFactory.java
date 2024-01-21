/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;

public class BioDataBufferFactory implements DataBufferFactory {

    private final BioWritableChannel channel;

    public BioDataBufferFactory(BioWritableChannel channel) {
        this.channel = channel;
    }

    @Override
    public DataBuffer create() {
        return channel.getDataBuffer(DataBuffer.MIN_GROW);
    }

    @Override
    public DataBuffer create(int capacity, boolean direct) {
        return channel.getDataBuffer(capacity);
    }

    @Override
    public void recycle(DataBuffer buffer) {
    }
}
