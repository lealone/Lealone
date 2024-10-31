/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;

public class NetBuffer {

    public static final int BUFFER_SIZE = 4 * 1024;

    private DataBuffer dataBuffer;
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

    public DataBuffer getDataBuffer() {
        return dataBuffer;
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

    public int position() {
        return dataBuffer.position();
    }

    public NetBuffer position(int newPosition) {
        dataBuffer.position(newPosition);
        return this;
    }

    public NetBuffer limit(int newLimit) {
        dataBuffer.limit(newLimit);
        return this;
    }

    private boolean global;

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    private int readIndex;

    public int getReadIndex() {
        return readIndex;
    }

    public void setReadIndex(int readIndex) {
        this.readIndex = readIndex;
    }

    private int packetCount;

    public int getPacketCount() {
        return packetCount;
    }

    public void incrementPacketCount() {
        packetCount++;
    }

    public void decrementPacketCount() {
        packetCount--;
    }

    public int remaining() {
        if (global)
            return dataBuffer.position() - readIndex;
        else
            return dataBuffer.getBuffer().remaining();
    }

    public void reset() {
        if (dataBuffer.capacity() > BUFFER_SIZE) {
            DataBufferFactory factory = dataBuffer.getFactory();
            if (factory == null)
                factory = DataBufferFactory.getConcurrentFactory();
            DataBuffer newBuffer = factory.create(BUFFER_SIZE, dataBuffer.getDirect());
            dataBuffer.close();
            dataBuffer = newBuffer;
        }
        readIndex = 0;
        packetCount = 0;
        dataBuffer.getBuffer().clear();
    }

    @Override
    public String toString() {
        return dataBuffer.getBuffer().toString();
    }

    public boolean isEmpty() {
        return remaining() == 0;
    }

    public boolean isNotEmpty() {
        return remaining() > 0;
    }
}
