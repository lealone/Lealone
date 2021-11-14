/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetServerBase;

//TODO 1.支持SSL 2.支持配置参数
public class NioNetServer extends NetServerBase {

    private static final Logger logger = LoggerFactory.getLogger(NioNetServer.class);
    private ServerSocketChannel serverChannel;

    @Override
    public boolean runInMainThread() {
        return runInMainThread;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        logger.info("Starting nio net server");
        try {
            serverChannel = ServerSocketChannel.open();
            serverChannel.socket().bind(new InetSocketAddress(getHost(), getPort()));
            serverChannel.configureBlocking(true);
            super.start();
            String name = getName() + "Accepter-" + getPort();
            if (runInMainThread()) {
                Thread t = Thread.currentThread();
                if (t.getName().equals("main"))
                    t.setName(name);
            } else {
                ConcurrentUtils.submitTask(name, isDaemon(), () -> {
                    NioNetServer.this.run();
                });
            }
        } catch (Exception e) {
            checkBindException(e, "Failed to start nio net server");
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        logger.info("Stopping nio net server");
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
    public Runnable getRunnable() {
        return () -> {
            NioNetServer.this.run();
        };
    }

    private void run() {
        while (!isStopped()) {
            accept();
        }
    }

    private void accept() {
        SocketChannel channel = null;
        AsyncConnection conn = null;
        try {
            channel = serverChannel.accept();
            channel.configureBlocking(false);
            NioWritableChannel writableChannel = new NioWritableChannel(channel, null);
            conn = createConnection(writableChannel, true);
        } catch (Throwable e) {
            if (conn != null) {
                removeConnection(conn);
            }
            closeChannel(channel);
            logger.warn(getName() + " failed to accept connection", e);
        }
    }

    static void closeChannel(SocketChannel channel) {
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
}
