/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.AsyncConnection;
import com.lealone.net.NetServerBase;

//负责接收新的ProtocolServer连接，比如TCP、P2P
//TODO 1.支持SSL 2.支持配置参数
public class NioNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(NioNetServer.class);
    private ServerSocketChannel serverChannel;

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.socket().bind(new InetSocketAddress(getHost(), getPort()));
            serverChannel.configureBlocking(false);
            connectionManager.registerAccepter(serverChannel);
        } catch (Exception e) {
            checkBindException(e, "Failed to start nio net server");
        }
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();
        if (serverChannel != null) {
            try {
                serverChannel.close();
                serverChannel = null;
            } catch (Throwable e) {
            }
        }
    }

    @Override
    public void accept(Scheduler scheduler) {
        AsyncConnection conn = null;
        NioWritableChannel writableChannel = null;
        try {
            SocketChannel channel = serverChannel.accept();
            channel.configureBlocking(false);
            writableChannel = new NioWritableChannel(scheduler, channel);
            conn = createConnection(writableChannel, scheduler);
        } catch (Throwable e) {
            if (conn != null) {
                removeConnection(conn);
            }
            if (writableChannel != null) {
                NioEventLoop.closeChannelSilently(writableChannel);
            }
            // 按Ctrl+C退出时accept可能抛出异常，此时就不需要记录日志了
            if (!isStopped()) {
                logger.warn(getName() + " failed to accept connection", e);
            }
        }
    }
}
