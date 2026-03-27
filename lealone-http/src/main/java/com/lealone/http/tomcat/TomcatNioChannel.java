/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.tomcat.util.net.NioChannel;
import org.apache.tomcat.util.net.SocketBufferHandler;

public class TomcatNioChannel extends NioChannel {

    private int batchCount;
    private int maxSize = 128 * 1024;
    private ByteBuffer writeBuffer = ByteBuffer.allocateDirect(maxSize * 2);

    public TomcatNioChannel(SocketBufferHandler bufHandler) {
        super(bufHandler);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int size = src.limit();
        if (size >= maxSize) {
            batchWrite(src);
        } else {
            writeBuffer.put(src);
            if (++batchCount > 100 || writeBuffer.position() > maxSize) {
                batchWrite(writeBuffer);
            }
        }
        return size;
    }

    public void batchWrite() throws IOException {
        if (batchCount > 0) {
            batchWrite(writeBuffer);
        }
    }

    private void batchWrite(ByteBuffer buff) throws IOException {
        // long t1 = System.nanoTime();
        buff.flip();
        batchCount = 0;
        while (buff.remaining() > 0)
            sc.write(buff);
        buff.clear();
        // System.out.println("batchWrite: " + (System.nanoTime() - t1) / 1000);
    }
}
