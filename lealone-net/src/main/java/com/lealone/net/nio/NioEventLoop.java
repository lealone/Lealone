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
import com.lealone.db.DataBuffer;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerThread;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetBuffer;
import com.lealone.net.NetBuffer.WritableBuffer;
import com.lealone.net.NetClient;
import com.lealone.net.NetEventLoop;
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
        WritableChannel channel = conn.getWritableChannel();
        addChannel(channel);
        try {
            SelectionKey key = channel.getSocketChannel().register(getSelector(), SelectionKey.OP_READ,
                    new NioAttachment(conn));
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
        int packetLength = attachment.packetLength; // 看看是不是上一次记下的packetLength
        int recyclePos = -1;
        try {
            if (conn.getPacketLengthByteCount() <= 0) { // http server自己读取数据
                conn.handle(null, false);
                return;
            }

            // 继续读上一个未读完的包
            if (attachment.inBuffer != null) {
                NetBuffer inBuffer = attachment.inBuffer;
                ByteBuffer buffer = inBuffer.getByteBuffer();
                if (!read(conn, channel, buffer) || buffer.remaining() > 0) {
                    return;
                } else {
                    attachment.inBuffer = null;
                    attachment.packetLength = 0;
                    buffer.flip();
                    if (packetLength == -1) { // 上次连packetLength都没有读出来，先计算packetLength
                        packetLength = conn.getPacketLength(buffer);
                        inBuffer.recycle();
                    } else {
                        conn.handle(inBuffer, false);
                        inBuffer.recycle();
                        return;
                    }
                }
            }

            // 以下是正常从全局inputBuffer中读数据

            NetBuffer inputBuffer = this.inputBuffer;
            ByteBuffer buffer = inputBuffer.getByteBuffer();
            if (buffer.remaining() == 0) {
                if (inputBuffer.getPacketCount() > 0) {
                    inputBuffer.getDataBuffer().growCapacity(DataBuffer.MIN_GROW);
                } else if (nestCount > 1) { // 嵌套读
                    inputBuffer = new NetBuffer(DataBuffer.createDirect());
                } else {
                    DbException.throwInternalError();
                }
                buffer = inputBuffer.getByteBuffer();
            }

            int start = buffer.position();
            recyclePos = start;
            if (!read(conn, channel, buffer)) {
                if (packetLength != 0)
                    attachment.packetLength = packetLength; // 记下packetLength
                return;
            }
            buffer.flip();
            if (start != 0)
                buffer.position(start); // 从上一个包的后续位置开始读

            while (true) {
                int remaining = buffer.remaining();
                if (remaining == 0) {
                    inputBuffer.recycle(recyclePos);
                    return;
                }
                // 读packetLength
                if (packetLength <= 0) {
                    // 每次循环重新取一次，一些实现会返回不同的值
                    int packetLengthByteCount = conn.getPacketLengthByteCount();
                    if (remaining >= packetLengthByteCount) {
                        packetLength = conn.getPacketLength(buffer);
                        remaining = buffer.remaining();
                    } else {
                        // 可用字节还不够读packetLength
                        attachment.packetLength = -1;
                        attachment.inBuffer = inputBuffer.createReadableBuffer(buffer.position(),
                                packetLengthByteCount);
                        attachment.inBuffer.position(remaining);
                        return;
                    }
                }
                if (remaining >= packetLength) {
                    int nextPos = buffer.position() + packetLength;
                    BioWritableChannel.checkPacketLength(maxPacketSize, packetLength);
                    conn.handle(inputBuffer, false);
                    // 如果没有完整读完一个包，直接跳过剩余的
                    if (buffer.position() != nextPos)
                        buffer.position(nextPos);
                    packetLength = 0;
                    attachment.packetLength = 0;
                } else {
                    if (packetLength != 0)
                        attachment.packetLength = packetLength; // 记下packetLength

                    if (remaining == 0) {
                        inputBuffer.recycle(recyclePos);
                    } else {
                        start = buffer.position();
                        // 如果是因为容量不够，扩容后再读一次
                        if (buffer.capacity() - start < packetLength) {
                            buffer = inputBuffer.getDataBuffer().growCapacity(packetLength);
                            buffer.position(start + remaining); // 从新的位置开始读
                            if (read(conn, channel, buffer)) {
                                buffer.flip();
                                buffer.position(start);
                                continue;
                            } else {
                                buffer.position(start); // 恢复位置
                            }
                        }
                        attachment.inBuffer = inputBuffer.createReadableBuffer(start, packetLength);
                        attachment.inBuffer.position(remaining);
                    }
                    return;
                }
            }
        } catch (Exception e) {
            // 发生异常时释放当前连接在共享inputBuffer中已经占用的内存
            if (recyclePos != -1)
                inputBuffer.recycle(recyclePos);
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
                batchWrite(channel);
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
        batchWrite(channel);
    }

    @Override
    public void write(WritableChannel channel, WritableBuffer buffer) {
        if (DbException.ASSERT) {
            DbException.assertTrue(scheduler == SchedulerThread.currentScheduler());
        }
        if (channel.isClosed()) {
            // 通道关闭了，recycle后就返回
            recycleBuffer(buffer);
            return;
        }
        boolean writeImmediately;
        if (channel.getBuffers().size() > 2 || needWriteImmediately()) {
            channel.addBuffer(buffer);
            batchWrite(channel);
            return;
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
                recycleBuffer(buffer);
                return;
            }
        }
        // 如果没有写完留到后面再写
        channel.addBuffer(buffer);
    }

    private void recycleBuffer(WritableBuffer buffer) {
        if (buffer != null) {
            buffer.recycle();
        }
    }

    private void batchWrite(WritableChannel channel) {
        List<WritableBuffer> buffers = channel.getBuffers();
        if (buffers != null && !buffers.isEmpty()) {
            SelectionKey key = channel.getSelectionKey();
            if (key != null && key.isValid()) {
                batchWrite(key, channel.getSocketChannel(), buffers);
            }
        }
    }

    private void batchWrite(SelectionKey key, SocketChannel channel, List<WritableBuffer> list) {
        int index = 0;
        int remaining = 0;
        ByteBuffer[] buffers = new ByteBuffer[list.size()];
        Iterator<WritableBuffer> iterator = list.iterator();
        while (iterator.hasNext()) {
            WritableBuffer buffer = iterator.next();
            remaining += buffer.getByteBuffer().remaining();
            buffers[index++] = buffer.getByteBuffer();
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
        NioWritableChannel.recycleBuffers(list);
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
            recycleBuffer(buffer);
            // 还是要检测key是否是有效的，否则会抛CancelledKeyException
            if (key.isValid() && (key.interestOps() & SelectionKey.OP_WRITE) != 0) {
                key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
            }
        } catch (IOException e) {
            recycleBuffer(buffer);
            handleWriteException(e, key);
        }
        return true;
    }

    private int nestCount;

    @Override
    public boolean isInLoop() {
        return nestCount > 0;
    }

    @Override
    public void handleSelectedKeys() {
        Set<SelectionKey> keys = getSelector().selectedKeys();
        if (!keys.isEmpty()) {
            if (nestCount > 0) {
                keys = new HashSet<>(keys); // 复制一份，避免并发修改异常
            }
            nestCount++;
            try {
                handleSelectedKeys(keys);
            } finally {
                nestCount--;
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
                    netClient.connectionEstablished(scheduler, this, key);
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
        NioWritableChannel.recycleBuffers(channel.getBuffers());
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
        AsyncConnection conn = ((NioAttachment) key.attachment()).conn;
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
