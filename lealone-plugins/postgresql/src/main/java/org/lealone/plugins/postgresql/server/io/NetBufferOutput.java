/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server.io;

import org.lealone.db.DataBufferFactory;
import org.lealone.net.NetBuffer;
import org.lealone.net.WritableChannel;

public class NetBufferOutput implements AutoCloseable {

    protected final WritableChannel writableChannel;
    protected final int initialSizeHint;
    protected final DataBufferFactory dataBufferFactory;
    protected NetBuffer buffer;

    public NetBufferOutput(WritableChannel writableChannel, int initialSizeHint,
            DataBufferFactory dataBufferFactory) {
        this.writableChannel = writableChannel;
        this.initialSizeHint = initialSizeHint;
        this.dataBufferFactory = dataBufferFactory;
        reset();
    }

    public void writeShort(int v) {
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void writeInt(int v) {
        write((v >>> 24) & 0xFF);
        write((v >>> 16) & 0xFF);
        write((v >>> 8) & 0xFF);
        write((v >>> 0) & 0xFF);
    }

    public void write(int b) {
        buffer.appendByte((byte) b);
    }

    public void write(byte b[]) {
        write(b, 0, b.length);
    }

    public void write(byte b[], int off, int len) {
        buffer.appendBytes(b, off, len);
    }

    public void setByte(int pos, byte b) {
        buffer.setByte(pos, b);
    }

    public void setInt(int pos, int v) {
        buffer.setByte(pos, (byte) ((v >>> 24) & 0xFF));
        buffer.setByte(pos + 1, (byte) ((v >>> 16) & 0xFF));
        buffer.setByte(pos + 2, (byte) ((v >>> 8) & 0xFF));
        buffer.setByte(pos + 3, (byte) (v & 0xFF));
    }

    public int length() {
        return buffer.length();
    }

    public void flush() {
        buffer.flip();
        NetBuffer old = buffer;
        reset();
        writableChannel.write(old);
        // 警告: 不能像下面这样用，调用write后会很快写数据到接收端，然后另一个线程很快又收到响应，
        // 在调用reset前又继续用原来的buffer写，从而导致产生非常难找的协议与并发问题，我就为这个问题痛苦排查过大半天。
        // writableChannel.write(buffer);
        // reset();
    }

    protected void reset() {
        buffer = writableChannel.getBufferFactory().createBuffer(initialSizeHint, dataBufferFactory);
    }

    @Override
    public void close() {
        buffer.recycle();
    }
}
