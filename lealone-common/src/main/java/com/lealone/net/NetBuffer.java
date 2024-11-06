/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.nio.ByteBuffer;

import com.lealone.db.DataBuffer;

public class NetBuffer {

    protected DataBuffer dataBuffer;
    protected int packetCount;

    public NetBuffer(DataBuffer dataBuffer) {
        this.dataBuffer = dataBuffer;
    }

    public int getPacketCount() {
        return packetCount;
    }

    public ByteBuffer getByteBuffer() {
        return dataBuffer.getBuffer();
    }

    public DataBuffer getDataBuffer() {
        return dataBuffer;
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

    public int length() {
        return dataBuffer.position();
    }

    public int getUnsignedByte() {
        return dataBuffer.getUnsignedByte();
    }

    public void recycle() {
        // 还有包要处理不能回收
        if (packetCount <= 0) {
            packetCount = 0;
            dataBuffer.clear();
        }
    }

    @Override
    public String toString() {
        return dataBuffer.getBuffer().toString();
    }

    public WritableBuffer createWritableBuffer(int start, int end) {
        dataBuffer.checkCapacity(end - start);
        ByteBuffer bb = dataBuffer.sliceByteBuffer(start, end);
        WritableBuffer buffer = new WritableBuffer(this, bb);
        packetCount++;
        return buffer;
    }

    public ReadableBuffer createReadableBuffer(int start, int packetLength) {
        dataBuffer.checkCapacity(packetLength);
        int pos = dataBuffer.position();
        int end = start + packetLength;
        DataBuffer slice = dataBuffer.slice(start, end);
        slice.position(pos - start);
        dataBuffer.clear();
        dataBuffer.position(end);
        ReadableBuffer buffer = new ReadableBuffer(this, slice);
        packetCount++;
        return buffer;
    }

    public boolean isGlobal() {
        return true;
    }

    public static class WritableBuffer {

        private NetBuffer parent;
        private ByteBuffer buffer;

        public WritableBuffer(NetBuffer parent, ByteBuffer buffer) {
            this.parent = parent;
            this.buffer = buffer;
        }

        public ByteBuffer getByteBuffer() {
            return buffer;
        }

        public void recycle() {
            if (parent != null) {
                if (--parent.packetCount == 0)
                    parent.recycle();
                parent = null;
                buffer = null;
            }
        }
    }

    private static class ReadableBuffer extends NetBuffer {

        private NetBuffer parent;

        public ReadableBuffer(NetBuffer parent, DataBuffer dataBuffer) {
            super(dataBuffer);
            this.parent = parent;
        }

        @Override
        public boolean isGlobal() {
            return false;
        }

        @Override
        public void recycle() {
            if (parent != null) {
                if (--parent.packetCount == 0)
                    parent.recycle();
                parent = null;
            }
        }
    }
}
