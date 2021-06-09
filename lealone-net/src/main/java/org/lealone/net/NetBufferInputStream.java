/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.io.IOException;
import java.io.InputStream;

public class NetBufferInputStream extends InputStream {

    protected final NetBuffer buffer;
    protected final int size;
    protected int pos;

    public NetBufferInputStream(NetBuffer buffer) {
        this.buffer = buffer;
        size = buffer.length();
    }

    @Override
    public int available() throws IOException {
        return size - pos;
    }

    @Override
    public int read() throws IOException {
        return buffer.getUnsignedByte(pos++);
    }

    @Override
    public void close() throws IOException {
        // buffer中只有一个包时回收才安全
        if (buffer.isOnlyOnePacket())
            buffer.recycle();
    }
}
