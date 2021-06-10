/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.net.NetBuffer;

public class NioBuffer implements NetBuffer {

    private final DataBuffer dataBuffer;
    private boolean onlyOnePacket;

    public NioBuffer(DataBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
    }

    public NioBuffer(DataBuffer dataBuffer, boolean onlyOnePacket) {
        this.dataBuffer = dataBuffer;
        this.onlyOnePacket = onlyOnePacket;
    }

    public ByteBuffer getAndFlipBuffer() {
        return dataBuffer.getAndFlipBuffer();
    }

    public ByteBuffer getByteBuffer() {
        return dataBuffer.getBuffer();
    }

    @Override
    public int length() {
        int pos = dataBuffer.position();
        if (pos > 0)
            return pos;
        else
            return dataBuffer.limit();
    }

    @Override
    public short getUnsignedByte(int pos) {
        return dataBuffer.getUnsignedByte(pos);
    }

    @Override
    public NioBuffer appendByte(byte b) {
        dataBuffer.put(b);
        return this;
    }

    @Override
    public NioBuffer appendBytes(byte[] bytes, int offset, int len) {
        dataBuffer.put(bytes, offset, len);
        return this;
    }

    @Override
    public NioBuffer appendInt(int i) {
        dataBuffer.putInt(i);
        return this;
    }

    @Override
    public NioBuffer setByte(int pos, byte b) {
        dataBuffer.putByte(pos, b);
        return this;
    }

    @Override
    public boolean isOnlyOnePacket() {
        return onlyOnePacket;
    }

    @Override
    public void recycle() {
        if (onlyOnePacket)
            dataBuffer.close();
    }

    @Override
    public NioBuffer flip() {
        dataBuffer.getAndFlipBuffer();
        return this;
    }
}
