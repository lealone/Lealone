/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server.io;

import org.lealone.net.NetBuffer;

public class NetBufferInput implements AutoCloseable {

    protected final NetBuffer buffer;
    protected final int size;
    protected int pos;

    public NetBufferInput(NetBuffer buffer) {
        this.buffer = buffer;
        size = buffer.length();
    }

    public int available() {
        return size - pos;
    }

    public int read() {
        return buffer.getUnsignedByte(pos++);
    }

    @Override
    public void close() {
        // buffer中只有一个包时回收才安全
        if (buffer.isOnlyOnePacket())
            buffer.recycle();
    }

    public byte readByte() {
        int ch = read();
        return (byte) ch;
    }

    public short readShort() {
        int ch1 = read();
        int ch2 = read();
        return (short) ((ch1 << 8) + (ch2 << 0));
    }

    public int readInt() {
        int ch1 = read();
        int ch2 = read();
        int ch3 = read();
        int ch4 = read();
        return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
    }

    public void readFully(byte b[]) {
        readFully(b, 0, b.length);
    }

    public final void readFully(byte b[], int off, int len) {
        for (int i = off; i < len; i++)
            b[i] = readByte();
    }
}
