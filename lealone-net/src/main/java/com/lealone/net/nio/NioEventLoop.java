/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.HashSet;
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

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.SystemPropertyUtils;
import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetClient;
import com.lealone.net.NetClientBase.ClientAttachment;
import com.lealone.net.NetEventLoop;
import com.lealone.net.TcpClientConnection;
import com.lealone.net.bio.BioWritableChannel;

public class NioEventLoop implements NetEventLoop {

    private static final Logger logger = LoggerFactory.getLogger(NioEventLoop.class);

    private final Map<SocketChannel, Queue<NetBuffer>> channels;
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
    private Scheduler scheduler;

    private final boolean isLoggerEnabled;
    private final boolean isDebugEnabled;

    public NioEventLoop(Map<String, String> config, long loopInterval, boolean isThreadSafe) {
        // 设置过大会占用内存，有可能影响GC暂停时间
        maxPacketCountPerLoop = MapUtils.getInt(config, "max_packet_count_per_loop", 8);
        maxPacketSize = BioWritableChannel.getMaxPacketSize(config);
        // client端不安全，所以不用批量写
        preferBatchWrite = MapUtils.getBoolean(config, "prefer_batch_write", isThreadSafe);

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
        // Selector.open()很慢，延迟初始化selector可以加快lealone的启动速度
        if (selector == null) {
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
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
                getSelector().select(timeout);
            }
            selecting.set(false);
        }
    }

    @Override
    public void wakeup() {
        haveWork = true;
        if (selecting.compareAndSet(true, false)) {
            Selector selector = this.selector;
            if (selector != null)
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
        Queue<NetBuffer> queue = channels.get(channel);
        if (queue != null) {
            if (!preferBatchWrite) {
                // 当队列不为空时，队首的NetBuffer可能没写完，此时不能写新的NetBuffer
                if ((isThreadSafe || SchedulerThread.currentScheduler() == scheduler)
                        && queue.isEmpty()) {
                    SelectionKey key = keyFor(channel);
                    if (key != null && key.isValid()) {
                        if (write(key, channel, netBuffer)) {
                            if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0) {
                                deregisterWrite(key);
                            }
                            return;
                        }
                    }
                }
            }
            writeQueueSize.incrementAndGet();
            queue.add(netBuffer);
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
                    BioWritableChannel.checkPacketLength(maxPacketSize, packetLength);
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
                        NetBuffer netBuffer = new NetBuffer(dataBuffer, true); // 支持快速回收
                        dataBuffer = null;
                        conn.handle(netBuffer);
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
            int oldSize = isThreadSafe ? channels.size() : 0;
            Iterator<Entry<SocketChannel, Queue<NetBuffer>>> iterator = channels.entrySet().iterator();
            while (iterator.hasNext()) {
                Entry<SocketChannel, Queue<NetBuffer>> entry = iterator.next();
                Queue<NetBuffer> queue = entry.getValue();
                if (!queue.isEmpty()) {
                    SocketChannel channel = entry.getKey();
                    SelectionKey key = keyFor(channel);
                    if (key != null && key.isValid()) {
                        write(key, channel, queue);
                    }
                }
                // 如果SocketChannel关闭了，会在channels中把它删除，
                // 此时调用iterator.next()会产生ConcurrentModificationException，
                // 所以在这里需要判断一下，若是发生变动了就创建新的iterator
                if (isThreadSafe && channels.size() != oldSize) {
                    oldSize = channels.size();
                    iterator = channels.entrySet().iterator();
                }
            }
        }
    }

    @Override
    public void write(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        Queue<NetBuffer> queue = channels.get(channel);
        if (!queue.isEmpty()) {
            write(key, channel, queue);
        }
    }

    private void write(SelectionKey key, SocketChannel channel, Queue<NetBuffer> queue) {
        if (preferBatchWrite) {
            batchWrite(key, channel, queue);
        } else {
            Iterator<NetBuffer> iterator = queue.iterator();
            while (iterator.hasNext()) {
                NetBuffer netBuffer = iterator.next();
                if (write(key, channel, netBuffer)) {
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

    private void batchWrite(SelectionKey key, SocketChannel channel, Queue<NetBuffer> queue) {
        int index = 0;
        int remaining = 0;
        ByteBuffer[] buffers = new ByteBuffer[queue.size()];
        Iterator<NetBuffer> iterator = queue.iterator();
        while (iterator.hasNext()) {
            NetBuffer netBuffer = iterator.next();
            remaining += netBuffer.getByteBuffer().remaining();
            buffers[index++] = netBuffer.getByteBuffer();
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
            NetBuffer netBuffer = iterator.next();
            netBuffer.recycle();
            iterator.remove();
            writeQueueSize.decrementAndGet();
        }
    }

    private boolean write(SelectionKey key, SocketChannel channel, NetBuffer netBuffer) {
        ByteBuffer buffer = netBuffer.getByteBuffer();
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
        netBuffer.recycle();
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
    public NetClient getNetClient() {
        return netClient;
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
            if (inLoop) {
                keys = new HashSet<>(keys); // 复制一份，避免并发修改异常
                handleSelectedKeys(keys);
                // DbException.throwInternalError();
            } else {
                try {
                    inLoop = true;
                    handleSelectedKeys(keys);
                } finally {
                    inLoop = false;
                }
            }
        }
    }

    private void handleSelectedKeys(Set<SelectionKey> keys) {
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
                    scheduler.accept(key);
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
    }

    @Override
    public synchronized void closeChannel(SocketChannel channel) {
        if (channel == null || !channels.containsKey(channel)) {
            return;
        }
        for (SelectionKey key : getSelector().keys()) {
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
    public synchronized void close() {
        try {
            // 正常关闭SocketChannel，避免让server端捕获到异常关闭信息
            for (SocketChannel channel : channels.keySet()) {
                closeChannel(channel);
            }
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
