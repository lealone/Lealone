package com.codefollower.lealone.hbase.tso.serialization;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BufferPool {

    private static BlockingQueue<ByteArrayOutputStream> pool = new LinkedBlockingQueue<ByteArrayOutputStream>();

    public static ByteArrayOutputStream getBuffer() {
        ByteArrayOutputStream baos = pool.poll();
        if (baos != null)
            return baos;
        return new ByteArrayOutputStream(1500);
    }

    public static void pushBuffer(ByteArrayOutputStream buffer) {
        pool.add(buffer);
    }
}
