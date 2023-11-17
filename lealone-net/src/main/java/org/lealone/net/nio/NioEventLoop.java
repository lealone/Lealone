/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.SystemPropertyUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.DataBufferFactory;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetBuffer;
import org.lealone.net.NetClient;
import org.lealone.net.NetClientBase.ClientAttachment;
import org.lealone.net.NetEventLoop;
import org.lealone.net.TcpClientConnection;

class NioEventLoop implements NetEventLoop {

    private static final Logger logger = LoggerFactory.getLogger(NioEventLoop.class);

    private final Map<SocketChannel, Queue<NioBuffer>> channels;
    private final Map<SocketChannel, SelectionKey> keys;

    private final AtomicInteger writeQueueSize = new AtomicInteger();
    private final AtomicBoolean selecting = new AtomicBoolean(false);
    private final long loopInterval;
    private final int maxPacketCountPerLoop; // 每次循环最多读取多少个数据包
    private int maxPacketSize;

    private final boolean isThreadSafe;
    private final DataBufferFactory dataBufferFactory;
    private boolean preferBatchWrite;

    private Selector selector;
    private NetClient netClient;
    private Accepter accepter;
    private Object owner;
    private Scheduler scheduler;

    private final boolean isLoggerEnabled;
    private final boolean isDebugEnabled;

    public NioEventLoop(Map<String, String> config, long loopInterval, boolean isThreadSafe) {
        // 设置过大会占用内存，有可能影响GC暂停时间
        maxPacketCountPerLoop = MapUtils.getInt(config, "max_packet_count_per_loop", 8);
        maxPacketSize = MapUtils.getInt(config, "max_packet_size", 8 * 1024 * 1024);
        // client端不安全，所以不用批量写
        preferBatchWrite = MapUtils.getBoolean(config, "prefer_batch_write", isThreadSafe);
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw DbException.convert(e);
        }

        this.loopInterval = loopInterval;
        this.isThreadSafe = isThreadSafe;
        if (isThreadSafe) {
            channels = new HashMap<>();
            keys = new HashMap<>();
            dataBufferFactory = DataBufferFactory.getSingleThreadFactory();
        } else {
            channels = new ConcurrentHashMap<>();
            keys = new ConcurrentHashMap<>();
            dataBufferFactory = DataBufferFactory.getConcurrentFactory();
        }

        isLoggerEnabled = SystemPropertyUtils.getBoolean("client_logger_enabled", true);
        isDebugEnabled = logger.isDebugEnabled() && isLoggerEnabled;
    }

    @Override
    public Object getOwner() {
        return owner;
    }

    @Override
    public void setOwner(Object owner) {
        this.owner = owner;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public void setPreferBatchWrite(boolean preferBatchWrite) {
        this.preferBatchWrite = preferBatchWrite;
    }

    @Override
    public DataBufferFactory getDataBufferFactory() {
        return dataBufferFactory;
    }

    @Override
    public Selector getSelector() {
        return selector;
    }

    @Override
    public void select() throws IOException {
        select(loopInterval);
    }

    private volatile boolean haveWork;

    @Override
    public void select(long timeout) throws IOException {
        if (selecting.compareAndSet(false, true)) {
            if (haveWork) {
                haveWork = false;
            } else {
                selector.select(timeout);
            }
            selecting.set(false);
        }
    }

    @Override
    public void wakeup() {
        haveWork = true;
        if (selecting.compareAndSet(true, false)) {
            selector.wakeup();
        }
    }

    @Override
    public void register(AsyncConnection conn) {
        NioAttachment attachment = new NioAttachment();
        attachment.conn = conn;
        SocketChannel channel = conn.getWritableChannel().getSocketChannel();
        addSocketChannel(channel);
        try {
            channel.register(getSelector(), SelectionKey.OP_READ, attachment);
        } catch (ClosedChannelException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void addSocketChannel(SocketChannel channel) {
        if (isThreadSafe)
            channels.putIfAbsent(channel, new LinkedList<>());
        else
            channels.putIfAbsent(channel, new ConcurrentLinkedQueue<>());
    }

    @Override
    public void addNetBuffer(SocketChannel channel, NetBuffer netBuffer) {
        NioBuffer nioBuffer = (NioBuffer) netBuffer;
        Queue<NioBuffer> queue = channels.get(channel);
        if (queue != null) {
            if (!preferBatchWrite) {
                SelectionKey key = keyFor(channel);
                // 当队列不为空时，队首的NioBuffer可能没写完，此时不能写新的NioBuffer
                if (key != null && key.isValid() && (isThreadSafe || Thread.currentThread() == owner)
                        && queue.isEmpty()) {
                    if (write(key, channel, nioBuffer)) {
                        if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0) {
                            deregisterWrite(key);
                        }
                        return;
                    }
                }
            }
            writeQueueSize.incrementAndGet();
            queue.add(nioBuffer);
            if (!isThreadSafe)
                wakeup();
        }
    }

    private SelectionKey keyFor(SocketChannel channel) {
        SelectionKey key = keys.get(channel);
        if (key == null) {
            key = channel.keyFor(getSelector());
            keys.put(channel, key);
        }
        return key;
    }

    private void registerWrite(SelectionKey key) {
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
    }

    private void deregisterWrite(SelectionKey key) {
        key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
    }

    private long totalReadBytes;
    private long totalWrittenBytes;
    private final EOFException endException = new EOFException();
    private String endExceptionMsg;

    @Override
    public void read(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        NioAttachment attachment = (NioAttachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        DataBuffer dataBuffer = attachment.dataBuffer;
        int packetCount = 1;
        try {
            while (true) {
                // 如果客户端关闭连接，服务器再次循环读数据检测到连接已经关闭就不再读取数据，避免抛出异常
                if (conn.isClosed())
                    return;
                // 每次循环重新取一次，一些实现会返回不同的Buffer
                ByteBuffer packetLengthByteBuffer = conn.getPacketLengthByteBuffer();
                if (packetLengthByteBuffer == null) { // http server自己读取数据
                    conn.handle(null);
                    return;
                }
                int packetLengthByteBufferCapacity = packetLengthByteBuffer.capacity();

                if (attachment.state == 0) {
                    boolean ok = read(attachment, channel, packetLengthByteBuffer,
                            packetLengthByteBufferCapacity);
                    if (ok) {
                        attachment.state = 1;
                    } else {
                        break;
                    }
                }
                if (attachment.state == 1) {
                    int packetLength = conn.getPacketLength();
                    if (packetLength > maxPacketSize)
                        throw new IOException("packet too large, maxPacketSize: " + maxPacketSize
                                + ", receive: " + packetLength);
                    if (dataBuffer == null) {
                        dataBuffer = dataBufferFactory.create(packetLength);
                        // 返回的DatBuffer的Capacity可能大于packetLength，所以设置一下limit，不会多读
                        dataBuffer.limit(packetLength);
                    }
                    ByteBuffer buffer = dataBuffer.getBuffer();
                    boolean ok = read(attachment, channel, buffer, packetLength);
                    if (ok) {
                        packetLengthByteBuffer.clear();
                        attachment.state = 0;
                        attachment.dataBuffer = null;
                        NioBuffer nioBuffer = new NioBuffer(dataBuffer, true); // 支持快速回收
                        dataBuffer = null;
                        conn.handle(nioBuffer);
                        if (++packetCount > maxPacketCountPerLoop)
                            break;
                    } else {
                        packetLengthByteBuffer.flip(); // 下次可以重新计算packetLength
                        attachment.dataBuffer = dataBuffer;
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (endException == e) {
                if (logger.isDebugEnabled())
                    logger.debug((conn.isServer() ? "Client " : "\r\nServer ") + endExceptionMsg);
                handleException(null, e, key); // 不输出错误
            } else {
                handleReadException(e, key);
            }
        }
    }

    private boolean read(NioAttachment attachment, SocketChannel channel, ByteBuffer buffer, int length)
            throws IOException {
        int readBytes = channel.read(buffer);
        if (readBytes > 0) {
            if (isDebugEnabled) {
                totalReadBytes += readBytes;
                logger.debug(("total read bytes: " + totalReadBytes));
            }
            attachment.endOfStreamCount = 0;
        } else {
            // 客户端非正常关闭时，可能会触发JDK的bug，导致run方法死循环，selector.select不会阻塞
            // netty框架在下面这个方法的代码中有自己的不同解决方案
            // io.netty.channel.nio.NioEventLoop.processSelectedKey
            if (readBytes < 0) {
                attachment.endOfStreamCount++;
                if (attachment.endOfStreamCount > 3) {
                    endExceptionMsg = "socket channel closed: " + channel.getRemoteAddress();
                    throw endException;
                }
            }
        }
        if (length == buffer.position()) {
            buffer.flip();
            return true;
        }
        return false;
    }

    @Override
    public void write() {
        if (writeQueueSize.get() > 0) {
            for (Entry<SocketChannel, Queue<NioBuffer>> entry : channels.entrySet()) {
                Queue<NioBuffer> queue = entry.getValue();
                if (!queue.isEmpty()) {
                    SocketChannel channel = entry.getKey();
                    SelectionKey key = keyFor(channel);
                    if (key != null && key.isValid()) {
                        write(key, channel, queue);
                    }
                }
            }
        }
    }

    @Override
    public void write(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        Queue<NioBuffer> queue = channels.get(channel);
        if (!queue.isEmpty()) {
            write(key, channel, queue);
        }
    }

    private void write(SelectionKey key, SocketChannel channel, Queue<NioBuffer> queue) {
        if (preferBatchWrite) {
            batchWrite(key, channel, queue);
        } else {
            Iterator<NioBuffer> iterator = queue.iterator();
            while (iterator.hasNext()) {
                NioBuffer nioBuffer = iterator.next();
                if (write(key, channel, nioBuffer)) {
                    iterator.remove();
                    writeQueueSize.decrementAndGet();
                } else {
                    break; // 只要有一个没有写完，后面的就先不用写了
                }
            }
        }
        // 还是要检测key是否是有效的，否则会抛CancelledKeyException
        if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0 && queue.isEmpty()) {
            deregisterWrite(key);
        }
    }

    private void batchWrite(SelectionKey key, SocketChannel channel, Queue<NioBuffer> queue) {
        int index = 0;
        int remaining = 0;
        ByteBuffer[] buffers = new ByteBuffer[queue.size()];
        Iterator<NioBuffer> iterator = queue.iterator();
        while (iterator.hasNext()) {
            NioBuffer nioBuffer = iterator.next();
            remaining += nioBuffer.getByteBuffer().remaining();
            buffers[index++] = nioBuffer.getByteBuffer();
        }
        try {
            while (remaining > 0) {
                long written = channel.write(buffers);
                remaining -= written;
                if (written <= 0) {
                    if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                        registerWrite(key);
                    }
                    return; // 还没有写完
                }
            }
        } catch (IOException e) {
            handleWriteException(e, key);
        }

        iterator = queue.iterator();
        while (iterator.hasNext()) {
            NioBuffer nioBuffer = iterator.next();
            nioBuffer.recycle();
            iterator.remove();
            writeQueueSize.decrementAndGet();
        }
    }

    private boolean write(SelectionKey key, SocketChannel channel, NioBuffer nioBuffer) {
        ByteBuffer buffer = nioBuffer.getByteBuffer();
        int remaining = buffer.remaining();
        try {
            // 一定要用while循环来写，否则会丢数据！
            while (remaining > 0) {
                int writtenBytes = channel.write(buffer);
                if (writtenBytes <= 0) {
                    if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                        registerWrite(key);
                    }
                    return false; // 还没有写完
                }
                remaining -= writtenBytes;
                if (isDebugEnabled) {
                    totalWrittenBytes += writtenBytes;
                    logger.debug(("total written bytes: " + totalWrittenBytes));
                }
            }
        } catch (IOException e) {
            handleWriteException(e, key);
        }
        nioBuffer.recycle();
        return true;
    }

    private void connectionEstablished(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.isConnectionPending())
            return;

        ClientAttachment attachment = (ClientAttachment) key.attachment();
        try {
            channel.finishConnect();
            addSocketChannel(channel);
            NioWritableChannel writableChannel = new NioWritableChannel(channel, this);
            AsyncConnection conn;
            if (attachment.connectionManager != null) {
                conn = attachment.connectionManager.createConnection(writableChannel, false, scheduler);
            } else {
                conn = new TcpClientConnection(writableChannel, netClient, attachment.maxSharedSize);
            }
            conn.setInetSocketAddress(attachment.inetSocketAddress);
            netClient.addConnection(attachment.inetSocketAddress, conn);
            attachment.conn = conn;
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(conn);
            }
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT);
            channel.register(getSelector(), SelectionKey.OP_READ, attachment);
        } catch (Exception e) {
            if (attachment.ac != null) {
                attachment.ac.setAsyncResult(e);
            }
        }
    }

    @Override
    public void setNetClient(NetClient netClient) {
        this.netClient = netClient;
    }

    @Override
    public void setAccepter(Accepter accepter) {
        this.accepter = accepter;
    }

    private boolean inLoop;

    @Override
    public boolean isInLoop() {
        return inLoop;
    }

    @Override
    public void handleSelectedKeys() {
        Set<SelectionKey> keys = getSelector().selectedKeys();
        if (!keys.isEmpty()) {
            if (inLoop)
                DbException.throwInternalError();
            try {
                inLoop = true;
                Iterator<SelectionKey> iterator = keys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if (key.isValid()) {
                        int readyOps = key.readyOps();
                        if ((readyOps & SelectionKey.OP_READ) != 0) {
                            read(key);
                        } else if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                            write(key);
                        } else if ((readyOps & SelectionKey.OP_ACCEPT) != 0) {
                            accepter.accept(key);
                        } else if ((readyOps & SelectionKey.OP_CONNECT) != 0) {
                            connectionEstablished(key);
                        } else {
                            key.cancel();
                        }
                    } else {
                        key.cancel();
                    }
                    iterator.remove();
                }
            } finally {
                inLoop = false;
            }
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
        keys.remove(channel);
        closeChannelSilently(channel);
    }

    @Override
    public void close() {
        try {
            Selector selector = this.selector;
            this.selector = null;
            selector.wakeup();
            selector.close();
        } catch (Exception e) {
        }
    }

    private void handleReadException(Exception e, SelectionKey key) {
        handleException("read", e, key);
    }

    private void handleWriteException(Exception e, SelectionKey key) {
        handleException("write", e, key);
    }

    private void handleException(String operation, Exception e, SelectionKey key) {
        NioAttachment attachment = (NioAttachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        if (operation != null && isLoggerEnabled)
            logger.warn("Failed to " + operation + " remote address[" + conn.getHostAndPort() + "]: "
                    + e.getMessage());
        conn.handleException(e);
        closeChannel(conn.getWritableChannel().getSocketChannel());
    }

    static void closeChannelSilently(SocketChannel channel) {
        if (channel != null) {
            Socket socket = channel.socket();
            if (socket != null) {
                try {
                    socket.close();
                } catch (Throwable e) {
                }
            }
            try {
                channel.close();
            } catch (Throwable e) {
            }
        }
    }

    @Override
    public boolean isQueueLarge() {
        return writeQueueSize.get() > maxPacketCountPerLoop;
    }
}
