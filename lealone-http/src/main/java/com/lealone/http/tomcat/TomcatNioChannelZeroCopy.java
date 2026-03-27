/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.tomcat.util.net.SocketBufferHandler;

public class TomcatNioChannelZeroCopy extends TomcatNioChannel {

    private int batchCount = 0;

    public TomcatNioChannelZeroCopy(SocketBufferHandler bufHandler) {
        super(bufHandler);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        ByteBuffer buff = socketWrapper.getSocketBufferHandler().getWriteBuffer();
        int size = buff.position();
        if (++batchCount == 10) {
            batchWrite(buff);
        }
        return size;
    }

    @Override
    public void batchWrite() throws IOException {
        if (batchCount > 0) {
            ByteBuffer buff = socketWrapper.getSocketBufferHandler().getWriteBuffer();
            batchWrite(buff);
        }
    }

    private void batchWrite(ByteBuffer buff) throws IOException {
        buff.flip();
        batchCount = 0;
        while (buff.remaining() > 0)
            sc.write(buff);
        buff.clear();
    }
}
