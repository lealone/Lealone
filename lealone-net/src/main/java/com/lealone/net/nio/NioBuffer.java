/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.net.NetBuffer;

public class NioBuffer implements NetBuffer {

    private final DataBuffer dataBuffer;
    private boolean onlyOnePacket;
    private boolean forWrite;

    public NioBuffer(DataBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
        this.forWrite = true;
    }

    public NioBuffer(DataBuffer dataBuffer, boolean onlyOnePacket) {
        this.dataBuffer = dataBuffer;
        this.onlyOnePacket = onlyOnePacket;
    }

    public ByteBuffer getAndFlipBuffer() {
        return dataBuffer.getAndFlipBuffer();
    }

    @Override
    public ByteBuffer getByteBuffer() {
        return dataBuffer.getBuffer();
    }

    @Override
    public int length() {
        if (forWrite)
            return dataBuffer.position();
        if (onlyOnePacket)
            return dataBuffer.limit();
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
    public void read(byte[] dst, int off, int len) {
        dataBuffer.read(dst, off, len);
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
        if (onlyOnePacket || forWrite)
            dataBuffer.close();
    }

    @Override
    public NioBuffer flip() {
        dataBuffer.getAndFlipBuffer();
        return this;
    }
}
