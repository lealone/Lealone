/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public interface DataBufferFactory {

    DataBuffer create();

    DataBuffer create(int capacity);

    void recycle(DataBuffer buffer);

    public static DataBufferFactory getConcurrentFactory() {
        return ConcurrentDataBufferFactory.INSTANCE;
    }

    public static class ConcurrentDataBufferFactory implements DataBufferFactory {

        private static final ConcurrentDataBufferFactory INSTANCE = new ConcurrentDataBufferFactory();
        private static final int maxPoolSize = 20;
        private final AtomicInteger poolSize = new AtomicInteger();
        private final ConcurrentLinkedQueue<DataBuffer> queue = new ConcurrentLinkedQueue<>();

        @Override
        public DataBuffer create() {
            DataBuffer buffer = queue.poll();
            if (buffer == null)
                buffer = new DataBuffer();
            else {
                buffer.clear();
                poolSize.decrementAndGet();
            }
            return buffer;
        }

        @Override
        public DataBuffer create(int capacity) {
            DataBuffer buffer = null;
            for (int i = 0, size = poolSize.get(); i < size; i++) {
                buffer = queue.poll();
                if (buffer == null) {
                    break;
                }
                if (buffer.capacity() < capacity) {
                    queue.offer(buffer); // 放到队列末尾
                } else {
                    buffer.clear();
                    poolSize.decrementAndGet();
                    return buffer;
                }
            }
            return DataBuffer.create(capacity);
        }

        @Override
        public void recycle(DataBuffer buffer) {
            if (buffer.capacity() <= DataBuffer.MAX_REUSE_CAPACITY) {
                if (poolSize.incrementAndGet() <= maxPoolSize) {
                    queue.offer(buffer);
                } else {
                    poolSize.decrementAndGet();
                }
            }
        }
    }
}
