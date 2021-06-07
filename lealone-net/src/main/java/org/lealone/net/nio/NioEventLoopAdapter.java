/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.db.DataBuffer;
import org.lealone.net.AsyncConnection;
import org.lealone.net.nio.NioNetServer.Attachment;

public class NioEventLoopAdapter implements NioEventLoop {

    private static final Logger logger = LoggerFactory.getLogger(NioEventLoopAdapter.class);

    private final ConcurrentHashMap<SocketChannel, ConcurrentLinkedQueue<NioBuffer>> channels = new ConcurrentHashMap<>();

    private final AtomicBoolean selecting = new AtomicBoolean(false);
    private Selector selector;
    private final long loopInterval;

    public NioEventLoopAdapter(Map<String, String> config, String loopIntervalKey, long loopIntervalDefaultValue)
            throws IOException {
        loopInterval = DateTimeUtils.getLoopInterval(config, loopIntervalKey, loopIntervalDefaultValue);
        selector = Selector.open();
    }

    @Override
    public NioEventLoop getDefaultNioEventLoopImpl() {
        return this;
    }

    @Override
    public Selector getSelector() {
        return selector;
    }

    @Override
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
        channels.putIfAbsent(channel, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void addNioBuffer(SocketChannel channel, NioBuffer nioBuffer) {
        ConcurrentLinkedQueue<NioBuffer> queue = channels.get(channel);
        if (queue != null) {
            queue.add(nioBuffer);
            wakeup();
        }
    }

    @Override
    public void tryRegisterWriteOperation(Selector selector) {
        for (Entry<SocketChannel, ConcurrentLinkedQueue<NioBuffer>> entry : channels.entrySet()) {
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

    private long totalReadBytes;
    private long totalWrittenBytes;
    private final boolean isDebugEnabled = logger.isDebugEnabled();

    @Override
    public void read(SelectionKey key, NioEventLoop nioEventLoop) {
        Attachment attachment = (Attachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            while (true) {
                DataBuffer dataBuffer = DataBuffer.create();
                ByteBuffer buffer = dataBuffer.getBuffer();
                int capacity = dataBuffer.capacity();
                int readBytes = channel.read(buffer);
                if (readBytes > 0) {
                    attachment.endOfStreamCount = 0;
                } else {
                    // 客户端非正常关闭时，可能会触发JDK的bug，导致run方法死循环，selector.select不会阻塞
                    // netty框架在下面这个方法的代码中有自己的不同解决方案
                    // io.netty.channel.nio.NioEventLoop.processSelectedKey
                    if (readBytes < 0) {
                        attachment.endOfStreamCount++;
                        if (attachment.endOfStreamCount > 3) {
                            closeChannel(channel);
                        }
                    }
                    break;
                }
                buffer.flip();
                if (isDebugEnabled) {
                    totalReadBytes += readBytes;
                    logger.debug(("total read bytes: " + totalReadBytes));
                }
                NioBuffer nioBuffer = new NioBuffer(dataBuffer);
                conn.handle(nioBuffer);
                // 说明没读满，可以直接退出循环了
                if (readBytes < capacity)
                    break;
            }
        } catch (Exception e) {
            nioEventLoop.handleException(conn, channel, e);
        }
    }

    @Override
    public void write(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        try {
            Queue<NioBuffer> queue = channels.get(channel);
            for (NioBuffer nioBuffer : queue) {
                ByteBuffer buffer = nioBuffer.getByteBuffer();
                int remaining = buffer.remaining();
                // 一定要用while循环来写，否则会丢数据！
                while (remaining > 0) {
                    int writtenBytes = channel.write(buffer);
                    if (writtenBytes <= 0) {
                        if (key.isValid()) {
                            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                        }
                        return;
                    }
                    remaining -= writtenBytes;
                    if (isDebugEnabled) {
                        totalWrittenBytes += writtenBytes;
                        logger.debug(("total written bytes: " + totalWrittenBytes));
                    }
                }
                queue.remove(nioBuffer);
                nioBuffer.recycle();
            }

            // 还是要检测key是否是有效的，否则会抛CancelledKeyException
            if (queue.isEmpty() && key.isValid()) {
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
        if (channel == null || !channels.containsKey(channel)) {
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
