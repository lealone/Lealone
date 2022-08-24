/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.IOException;
import java.io.OutputStream;

import org.lealone.db.DataBufferFactory;

public class NetBufferOutputStream extends OutputStream {

    protected final WritableChannel writableChannel;
    protected final int initialSizeHint;
    protected final DataBufferFactory dataBufferFactory;
    protected NetBuffer buffer;

    public NetBufferOutputStream(WritableChannel writableChannel, int initialSizeHint,
            DataBufferFactory dataBufferFactory) {
        this.writableChannel = writableChannel;
        this.initialSizeHint = initialSizeHint;
        this.dataBufferFactory = dataBufferFactory;
        reset();
    }

    @Override
    public void write(int b) {
        buffer.appendByte((byte) b);
    }

    @Override
    public void write(byte b[], int off, int len) {
        buffer.appendBytes(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        flush(true);
    }

    public void flush(boolean reset) throws IOException {
        buffer.flip();
        if (reset) {
            NetBuffer old = buffer;
            reset();
            writableChannel.write(old);
            // 警告: 不能像下面这样用，调用write后会很快写数据到接收端，然后另一个线程很快又收到响应，
            // 在调用reset前又继续用原来的buffer写，从而导致产生非常难找的协议与并发问题，我就为这个问题痛苦排查过大半天。
            // writableChannel.write(buffer);
            // reset();
        } else {
            writableChannel.write(buffer);
            buffer = null;
        }
    }

    protected void reset() {
        buffer = writableChannel.getBufferFactory().createBuffer(initialSizeHint, dataBufferFactory);
    }
}
