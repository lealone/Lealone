/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.protocol;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.db.DataBufferFactory;
import org.lealone.net.WritableChannel;

public class PacketOutput {

    private static final int BUFFER_SIZE = 8 * 1024;

    private final WritableChannel writableChannel;
    private final DataBufferFactory dataBufferFactory;
    private DataBuffer dataBuffer;

    public PacketOutput(WritableChannel writableChannel, DataBufferFactory dataBufferFactory) {
        this.writableChannel = writableChannel;
        this.dataBufferFactory = dataBufferFactory;
    }

    public ByteBuffer allocate(int capacity) {
        capacity = Math.min(capacity, BUFFER_SIZE);
        dataBuffer = dataBufferFactory.create(capacity);
        return dataBuffer.getBuffer();
    }

    public ByteBuffer writeToBuffer(byte[] src, ByteBuffer buffer) {
        int offset = 0;
        int length = src.length;
        int remaining = buffer.remaining();
        while (length > 0) {
            if (remaining >= length) {
                buffer.put(src, offset, length);
                break;
            } else {
                buffer.put(src, offset, remaining);
                flush();
                buffer = allocate(BUFFER_SIZE);
                offset += remaining;
                length -= remaining;
                remaining = buffer.remaining();
                continue;
            }
        }
        return buffer;
    }

    public void flush() {
        if (dataBuffer != null) {
            dataBuffer.getAndFlipBuffer();
            writableChannel.write(writableChannel.getBufferFactory().createBuffer(dataBuffer));
            dataBuffer = null;
        }
    }
}
