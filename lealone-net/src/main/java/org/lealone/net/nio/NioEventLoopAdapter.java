/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net.nio;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.util.DateTimeUtils;

public class NioEventLoopAdapter implements NioEventLoop {

    private final ConcurrentHashMap<SocketChannel, ConcurrentLinkedQueue<ByteBuffer>> channels = new ConcurrentHashMap<>();

    private final AtomicBoolean selecting = new AtomicBoolean(false);
    private Selector selector;
    private final long loopInterval;

    public NioEventLoopAdapter(Map<String, String> config, String loopIntervalKey, long loopIntervalDefaultValue)
            throws IOException {
        loopInterval = DateTimeUtils.getLoopInterval(config, loopIntervalKey, loopIntervalDefaultValue);
        selector = Selector.open();
    }

    public Selector getSelector() {
        return selector;
    }

    public void select() throws IOException {
        select(loopInterval);
    }

    @Override
    public void select(long timeout) throws IOException {
        tryRegisterWriteOperation(selector);
        if (selecting.compareAndSet(false, true)) {
            selector.select(timeout);
            selecting.set(false);
        }
    }

    @Override
    public void register(SocketChannel channel, int ops, Object att) throws ClosedChannelException {
        // 当nio-event-loop线程执行selector.select被阻塞时，代码内部依然会占用publicKeys锁，
        // 而另一个线程执行channel.register时，内部也会去要publicKeys锁，从而导致也被阻塞，
        // 所以下面这段代码的用处是:
        // 只要发现nio-event-loop线程正在进行select，那么就唤醒它，并释放publicKeys锁。
        while (true) {
            if (selecting.compareAndSet(false, true)) {
                channel.register(selector, SelectionKey.OP_CONNECT, att);
                selecting.set(false);
                selector.wakeup();
                break;
            } else {
                selector.wakeup();
            }
        }
    }

    @Override
    public void wakeup() {
        if (selecting.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    public void addSocketChannel(SocketChannel channel) {
        ConcurrentLinkedQueue<ByteBuffer> queue = new ConcurrentLinkedQueue<>();
        channels.putIfAbsent(channel, queue);
    }

    @Override
    public void addNioBuffer(SocketChannel channel, NioBuffer nioBuffer) {
        ConcurrentLinkedQueue<ByteBuffer> queue = channels.get(channel);
        if (queue != null) {
            ByteBuffer buffer = nioBuffer.getByteBuffer();
            queue.add(buffer);
            wakeup();
        }
    }

    @Override
    public void tryRegisterWriteOperation(Selector selector) {
        for (Entry<SocketChannel, ConcurrentLinkedQueue<ByteBuffer>> entry : channels.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                for (SelectionKey key : selector.keys()) {
                    if (key.channel() == entry.getKey() && key.isValid()) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void write(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            Queue<ByteBuffer> queue = channels.get(channel);
            outer: for (ByteBuffer buffer : queue) {
                // 一定要用while循环来写，否则会丢数据！
                while (buffer.hasRemaining()) {
                    if (channel.write(buffer) <= 0)
                        break outer;
                }
                if (!buffer.hasRemaining()) {
                    queue.remove(buffer);
                }
            }
            if (queue.isEmpty()) {
                int ops = key.interestOps();
                ops &= ~SelectionKey.OP_WRITE;
                key.interestOps(ops);
            }
        } catch (IOException e) {
            closeChannel(channel);
        }
    }

    @Override
    public void closeChannel(SocketChannel channel) {
        if (channel == null) {
            return;
        }
        for (SelectionKey key : selector.keys()) {
            if (key.channel() == channel && key.isValid()) {
                key.cancel();
                break;
            }
        }
        channels.remove(channel);
        Socket socket = channel.socket();
        if (socket != null) {
            try {
                socket.close();
            } catch (Exception e) {
            }
        }
        try {
            channel.close();
        } catch (Exception e) {
        }
    }

    public void close() {
        try {
            Selector selector = this.selector;
            this.selector = null;
            selector.wakeup();
            selector.close();
        } catch (Exception e) {
        }
    }
}
