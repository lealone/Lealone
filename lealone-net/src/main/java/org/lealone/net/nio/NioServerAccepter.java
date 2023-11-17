/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetServerBase;

//负责接收新的ProtocolServer连接，比如TCP、P2P
//TODO 1.支持SSL 2.支持配置参数
class NioServerAccepter extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(NioServerAccepter.class);
    private ServerSocketChannel serverChannel;

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        super.start();
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.socket().bind(new InetSocketAddress(getHost(), getPort()));
            serverChannel.configureBlocking(false);
            connectionManager.registerAccepter(serverChannel);
        } catch (Exception e) {
            checkBindException(e, "Failed to start protocol server accepter");
        }
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
        SocketChannel channel = null;
        AsyncConnection conn = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            NioWritableChannel writableChannel = new NioWritableChannel(channel, null);
            conn = createConnection(writableChannel, scheduler);
        } catch (Throwable e) {
            if (conn != null) {
                removeConnection(conn);
            }
            NioEventLoop.closeChannelSilently(channel);
            // 按Ctrl+C退出时accept可能抛出异常，此时就不需要记录日志了
            if (!isStopped()) {
                logger.warn(getName() + " failed to accept connection", e);
            }
        }
    }
}
