/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.bio;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;
import com.lealone.net.NetBuffer;

public class BioDataBufferFactory implements DataBufferFactory {

    public static final BioDataBufferFactory INSTANCE = new BioDataBufferFactory();

    @Override
    public DataBuffer create() {
        return create(NetBuffer.BUFFER_SIZE);
    }

    @Override
    public DataBuffer create(int capacity) {
        // Socket的输入输出流只接受字节数组，而direct buffer是没有字节数组的，所以不能创建direct buffer
        // 见BioWritableChannel中的read和write方法的实现
        return DataBufferFactory.getConcurrentFactory().create(capacity, false);
    }

    @Override
    public DataBuffer create(int capacity, boolean direct) {
        return create(capacity);
    }

    @Override
    public void recycle(DataBuffer buffer) {
        DataBufferFactory.getConcurrentFactory().recycle(buffer);
    }
}
