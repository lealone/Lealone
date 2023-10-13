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

    default DataBuffer create(int capacity) {
        return create(capacity, true);
    }

    DataBuffer create(int capacity, boolean direct);

    void recycle(DataBuffer buffer);

    public static DataBufferFactory getSingleThreadFactory() {
        return new SingleThreadDataBufferFactory();
    }

    public static DataBufferFactory getConcurrentFactory() {
        return ConcurrentDataBufferFactory.INSTANCE;
    }

    public static class SingleThreadDataBufferFactory implements DataBufferFactory {

        private static final int maxPoolSize = 20;
        private final DataBuffer[] queue = new DataBuffer[maxPoolSize];

        @Override
        public DataBuffer create() {
            return create(0, true);
        }

        @Override
        public DataBuffer create(int capacity, boolean direct) {
            for (int i = 0; i < maxPoolSize; i++) {
                DataBuffer buffer = queue[i];
                if (buffer != null && (buffer.getDirect() != direct || buffer.capacity() >= capacity)) {
                    queue[i] = null;
                    buffer.clear();
                    return buffer;
                }
            }
            if (capacity <= 0)
                capacity = DataBuffer.MIN_GROW;
            DataBuffer buffer = DataBuffer.create(null, capacity, direct);
            buffer.setFactory(this);
            return buffer;
        }

        @Override
        public void recycle(DataBuffer buffer) {
            if (buffer.capacity() <= DataBuffer.MAX_REUSE_CAPACITY) {
                for (int i = 0; i < maxPoolSize; i++) {
                    if (queue[i] == null) {
                        queue[i] = buffer;
                        return;
                    }
                }
            }
            buffer.setFactory(null);
        }
    }

    public static class ConcurrentDataBufferFactory implements DataBufferFactory {

        private static final ConcurrentDataBufferFactory INSTANCE = new ConcurrentDataBufferFactory();

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
        public DataBuffer create(int capacity, boolean direct) {
            DataBuffer buffer = null;
            for (int i = 0, size = poolSize.get(); i < size; i++) {
                buffer = queue.poll();
                if (buffer == null) {
                    break;
                }
                if (buffer.getDirect() != direct || buffer.capacity() < capacity) {
                    queue.offer(buffer); // 放到队列末尾
                } else {
                    buffer.clear();
                    poolSize.decrementAndGet();
                    return buffer;
                }
            }
            return DataBuffer.create(null, capacity, direct);
        }

        @Override
        public void recycle(DataBuffer buffer) {
            if (buffer.capacity() <= DataBuffer.MAX_REUSE_CAPACITY) {
                if (poolSize.incrementAndGet() <= SingleThreadDataBufferFactory.maxPoolSize) {
                    queue.offer(buffer);
                } else {
                    poolSize.decrementAndGet();
                }
            }
        }
    }
}
