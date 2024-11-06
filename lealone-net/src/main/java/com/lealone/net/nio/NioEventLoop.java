/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.SystemPropertyUtils;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetBuffer.WritableBuffer;
import com.lealone.net.NetClient;
import com.lealone.net.NetClientBase.ClientAttachment;
import com.lealone.net.NetEventLoop;
import com.lealone.net.TcpClientConnection;
import com.lealone.net.WritableChannel;
import com.lealone.net.bio.BioWritableChannel;
import com.lealone.server.ProtocolServer;

public class NioEventLoop implements NetEventLoop {

    private static final Logger logger = LoggerFactory.getLogger(NioEventLoop.class);

    private final Map<WritableChannel, WritableChannel> channels = new HashMap<>();
    private final AtomicBoolean selecting = new AtomicBoolean(false);

    private final Scheduler scheduler;
    private final NetBuffer inputBuffer;
    private final NetBuffer outputBuffer;
    private final long loopInterval;
    private final int maxPacketCountPerLoop; // 每次循环最多读取多少个数据包
    private final int maxPacketSize;

    private boolean preferBatchWrite;

    private Selector selector;
    private NetClient netClient; // 在客户端的场景才有

    private final boolean isLoggerEnabled;
    private final boolean isDebugEnabled;

    public NioEventLoop(Scheduler scheduler, long loopInterval, Map<String, String> config) {
        this.scheduler = scheduler;
        this.inputBuffer = scheduler.getInputBuffer();
        this.outputBuffer = scheduler.getOutputBuffer();
        this.loopInterval = loopInterval;
        // 设置过大会占用内存，有可能影响GC暂停时间
        maxPacketCountPerLoop = MapUtils.getInt(config, "max_packet_count_per_loop", 8);
        maxPacketSize = BioWritableChannel.getMaxPacketSize(config);
        preferBatchWrite = MapUtils.getBoolean(config, "prefer_batch_write", true);

        isLoggerEnabled = SystemPropertyUtils.getBoolean("client_logger_enabled", true);
        isDebugEnabled = logger.isDebugEnabled() && isLoggerEnabled;
    }

    @Override
    public void setNetClient(NetClient netClient) {
        this.netClient = netClient;
    }

    @Override
    public NetClient getNetClient() {
        return netClient;
    }

    @Override
    public void setPreferBatchWrite(boolean preferBatchWrite) {
        this.preferBatchWrite = preferBatchWrite;
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
    public void wakeUp() {
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
        WritableChannel channel = conn.getWritableChannel();
        addChannel(channel);
        try {
            SelectionKey key = channel.getSocketChannel().register(getSelector(), SelectionKey.OP_READ,
                    attachment);
            channel.setSelectionKey(key);
        } catch (ClosedChannelException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public void addChannel(WritableChannel channel) {
        channels.put(channel, channel);
    }

    private long totalReadBytes;
    private long totalWrittenBytes;

    @Override
    public void read(SelectionKey key) {
        NioAttachment attachment = (NioAttachment) key.attachment();
        AsyncConnection conn = attachment.conn;
        // 如果客户端关闭连接，服务器再次循环读数据检测到连接已经关闭就不再读取数据，避免抛出异常
        if (conn.isClosed())
            return;
        SocketChannel channel = (SocketChannel) key.channel();
        NetBuffer inBuffer = attachment.inBuffer;
        try {
            // 每次循环重新取一次，一些实现会返回不同的limit
            int packetLengthByteCount = conn.getPacketLengthByteCount();
            if (packetLengthByteCount <= 0) { // http server自己读取数据
                conn.handle(null, false);
                return;
            }
            int packetLength = attachment.state; // 看看是不是上一次记下的packetLength
            if (attachment.state == -1) {
                ByteBuffer buffer = inBuffer.getByteBuffer();
                if (!read(conn, channel, buffer) || buffer.position() < packetLengthByteCount) {
                    return;
                } else {
                    buffer.flip();
                    packetLength = conn.getPacketLength(buffer);
                    attachment.inBuffer = null;
                    attachment.state = 0;
                    inBuffer.recycle();
                    inBuffer = null;
                }
            }
            if (attachment.state >= 0) {
                if (inBuffer == null) {
                    inBuffer = inputBuffer;
                }
                ByteBuffer buffer = inBuffer.getByteBuffer();
                int start = buffer.position();
                if (!read(conn, channel, buffer)) {
                    if (packetLength != 0)
                        attachment.state = packetLength; // 记下packetLength
                    return;
                }
                buffer.flip();
                if (start != 0 && inBuffer.isGlobal())
                    buffer.position(start); // 从上一个包的后续位置开始读
                while (true) {
                    int remaining = buffer.remaining();
                    if (remaining >= packetLengthByteCount) {
                        if (packetLength == 0) {
                            packetLength = conn.getPacketLength(buffer);
                            remaining = buffer.remaining();
                        }
                        BioWritableChannel.checkPacketLength(maxPacketSize, packetLength);
                        if (remaining >= packetLength) {
                            attachment.state = 0;
                            attachment.inBuffer = null;
                            packetLength = 0;
                            conn.handle(inBuffer, false);
                        } else {
                            start = buffer.position();
                            attachment.state = packetLength;
                            attachment.inBuffer = inBuffer.createReadableBuffer(start, packetLength);
                            attachment.inBuffer.position(remaining);
                            return;
                        }
                    } else {
                        if (remaining == 0) {
                            inBuffer.recycle();
                        } else {
                            start = buffer.position();
                            attachment.state = -1;
                            attachment.inBuffer = inBuffer.createReadableBuffer(start,
                                    packetLengthByteCount);
                            attachment.inBuffer.position(remaining);
                        }
                        return;
                    }
                }
            }
        } catch (Exception e) {
            handleReadException(e, key);
        }
    }

    private boolean read(AsyncConnection conn, SocketChannel channel, ByteBuffer buffer)
            throws IOException {
        int readBytes = channel.read(buffer);
        if (readBytes > 0) {
            if (isDebugEnabled) {
                totalReadBytes += readBytes;
                logger.debug("total read bytes: " + totalReadBytes);
            }
            return true;
        } else {
            // 客户端非正常关闭时，可能会触发JDK的bug，导致run方法死循环，selector.select不会阻塞
            // netty框架在下面这个方法的代码中有自己的不同解决方案
            // io.netty.channel.nio.NioEventLoop.processSelectedKey
            if (readBytes < 0) {
                if (isDebugEnabled) {
                    logger.debug("socket channel closed: " + channel.getRemoteAddress());
                }
                conn.close();
            }
            return false;
        }
    }

    @Override
    public void write() {
        if (outputBuffer.getPacketCount() > 0) {
            int oldSize = channels.size();
            Iterator<WritableChannel> iterator = channels.keySet().iterator();
            while (iterator.hasNext()) {
                WritableChannel channel = iterator.next();
                List<WritableBuffer> buffers = channel.getBuffers();
                if (buffers != null && !buffers.isEmpty()) {
                    SelectionKey key = channel.getSelectionKey();
                    if (key != null && key.isValid()) {
                        batchWrite(key, channel.getSocketChannel(), buffers);
                    }
                }
                // 如果SocketChannel关闭了，会在channels中把它删除，
                // 此时调用iterator.next()会产生ConcurrentModificationException，
                // 所以在这里需要判断一下，若是发生变动了就创建新的iterator
                if (channels.size() != oldSize) {
                    oldSize = channels.size();
                    iterator = channels.keySet().iterator();
                }
            }
        }
    }

    @Override
    public void write(SelectionKey key) {
        WritableChannel channel = ((NioAttachment) key.attachment()).conn.getWritableChannel();
        List<WritableBuffer> buffers = channel.getBuffers();
        if (buffers != null && !buffers.isEmpty()) {
            batchWrite(key, channel.getSocketChannel(), buffers);
        }
    }

    @Override
    public void write(WritableChannel channel, WritableBuffer buffer) {
        if (DbException.ASSERT) {
            DbException.assertTrue(scheduler == SchedulerThread.currentScheduler());
        }
        if (channel.isClosed()) {
            // 通道关闭了，reset后就返回
            resetBuffer(buffer);
            return;
        }
        boolean writeImmediately;
        if (channel.getBuffers().size() > 2 || needWriteImmediately()) {
            writeImmediately = true;
        } else if (scheduler.isBusy()) {
            writeImmediately = false;
        } else {
            writeImmediately = !preferBatchWrite;
        }
        if (writeImmediately) {
            SelectionKey key = channel.getSelectionKey();
            if (key != null && key.isValid()) {
                if (write(key, channel.getSocketChannel(), buffer))
                    return;
            } else {
                resetBuffer(buffer);
                return;
            }
        }
        // 如果没有写完也增加计数，留到后面再写
        channel.addBuffer(buffer);
    }

    private void resetBuffer(WritableBuffer buffer) {
        if (buffer != null) {
            buffer.recycle();
        }
    }

    private void resetBuffer(List<WritableBuffer> buffers) {
        if (buffers != null) {
            for (WritableBuffer buffer : buffers)
                buffer.recycle();
        }
    }

    private void batchWrite(SelectionKey key, SocketChannel channel, List<WritableBuffer> list) {
        int index = 0;
        int remaining = 0;
        ByteBuffer[] buffers = new ByteBuffer[list.size()];
        Iterator<WritableBuffer> iterator = list.iterator();
        while (iterator.hasNext()) {
            WritableBuffer netBuffer = iterator.next();
            remaining += netBuffer.getByteBuffer().remaining();
            buffers[index++] = netBuffer.getByteBuffer();
        }
        try {
            while (remaining > 0) {
                long written = channel.write(buffers);
                remaining -= written;
                if (written <= 0) {
                    if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    }
                    return; // 还没有写完
                }
            }
        } catch (IOException e) {
            handleWriteException(e, key);
        }

        iterator = list.iterator();
        while (iterator.hasNext()) {
            WritableBuffer netBuffer = iterator.next();
            netBuffer.recycle();
            iterator.remove();
        }
    }

    private boolean write(SelectionKey key, SocketChannel channel, WritableBuffer buffer) {
        ByteBuffer bb = buffer.getByteBuffer();
        int remaining = bb.remaining();
        try {
            // 一定要用while循环来写，否则会丢数据！
            while (remaining > 0) {
                long written = channel.write(bb);
                remaining -= written;
                if (written <= 0) {
                    if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) == 0) {
                        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                    }
                    return false; // 还没有写完
                }
                if (isDebugEnabled) {
                    totalWrittenBytes += written;
                    logger.debug(("total written bytes: " + totalWrittenBytes));
                }
            }
            resetBuffer(buffer);
            // 还是要检测key是否是有效的，否则会抛CancelledKeyException
            if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            resetBuffer(buffer);
            handleWriteException(e, key);
        }
        return true;
    }

    private void connectionEstablished(SelectionKey key) {
        SocketChannel channel = (SocketChannel) key.channel();
        if (!channel.isConnectionPending())
            return;

        ClientAttachment attachment = (ClientAttachment) key.attachment();
        try {
            channel.finishConnect();
            NioWritableChannel writableChannel = new NioWritableChannel(scheduler, channel);
            writableChannel.setSelectionKey(key);
            AsyncConnection conn;
            if (attachment.connectionManager != null) {
                conn = attachment.connectionManager.createConnection(writableChannel, false, scheduler);
            } else {
                NetBuffer inBuffer = scheduler.getInputBuffer();
                NetBuffer outBuffer = scheduler.getOutputBuffer();
                conn = new TcpClientConnection(writableChannel, netClient, attachment.maxSharedSize,
                        inBuffer, outBuffer);
            }
            addChannel(writableChannel);
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
                    ProtocolServer server = (ProtocolServer) key.attachment();
                    if (server != null)
                        server.accept(scheduler);
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
    public void closeChannel(WritableChannel channel) {
        if (channel == null || !channels.containsKey(channel)) {
            return;
        }
        resetBuffer(channel.getBuffers());
        SelectionKey key = channel.getSelectionKey();
        if (key != null && key.isValid())
            key.cancel();
        channels.remove(channel);
        closeChannelSilently(channel);
    }

    @Override
    public void close() {
        try {
            // 正常关闭SocketChannel，避免让server端捕获到异常关闭信息
            // copy一份channels.keySet()，避免ConcurrentModificationException
            for (WritableChannel channel : new ArrayList<>(channels.keySet())) {
                closeChannel(channel);
            }
        } catch (Throwable t) {
        }
        if (this.selector != null) {
            try {
                Selector selector = this.selector;
                this.selector = null;
                selector.wakeup();
                selector.close();
            } catch (Throwable t) {
            }
        }
        if (this.netClient != null) {
            try {
                NetClient netClient = this.netClient;
                this.netClient = null;
                netClient.close();
            } catch (Throwable t) {
            }
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
        closeChannel(conn.getWritableChannel());
    }

    static void closeChannelSilently(WritableChannel channel) {
        if (channel != null) {
            Socket socket = channel.getSocketChannel().socket();
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
    public boolean needWriteImmediately() {
        return outputBuffer.getPacketCount() > maxPacketCountPerLoop;
    }
}
