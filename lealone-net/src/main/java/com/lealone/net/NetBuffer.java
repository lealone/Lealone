/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;

public class NetBuffer {

    private final DataBuffer dataBuffer;
    private boolean onlyOnePacket;
    private boolean forWrite;

    public NetBuffer(DataBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
        this.forWrite = true;
    }

    public NetBuffer(DataBuffer dataBuffer, boolean onlyOnePacket) {
        this.dataBuffer = dataBuffer;
        this.onlyOnePacket = onlyOnePacket;
    }

    public ByteBuffer getAndFlipBuffer() {
        return dataBuffer.getAndFlipBuffer();
    }

    public ByteBuffer getByteBuffer() {
        return dataBuffer.getBuffer();
    }

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

    public short getUnsignedByte(int pos) {
        return dataBuffer.getUnsignedByte(pos);
    }

    public void read(byte[] dst, int off, int len) {
        dataBuffer.read(dst, off, len);
    }

    public NetBuffer appendByte(byte b) {
        dataBuffer.put(b);
        return this;
    }

    public NetBuffer appendBytes(byte[] bytes, int offset, int len) {
        dataBuffer.put(bytes, offset, len);
        return this;
    }

    public NetBuffer appendInt(int i) {
        dataBuffer.putInt(i);
        return this;
    }

    public NetBuffer setByte(int pos, byte b) {
        dataBuffer.putByte(pos, b);
        return this;
    }

    public boolean isOnlyOnePacket() {
        return onlyOnePacket;
    }

    public void recycle() {
        if (onlyOnePacket || forWrite)
            dataBuffer.close();
    }

    public NetBuffer flip() {
        dataBuffer.getAndFlipBuffer();
        return this;
    }
}
