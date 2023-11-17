/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.MapUtils;
import org.lealone.common.util.ThreadUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.AsyncTask;
import org.lealone.net.AsyncConnection;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetEventLoop;

class NioClient extends NetClientBase {

    private static final Logger logger = LoggerFactory.getLogger(NioClient.class);

    private final CopyOnWriteArrayList<AsyncTask> registerConnectTasks = new CopyOnWriteArrayList<>();
    private final int id;
    private NioEventLoop nioEventLoop;

    NioClient(int id) {
        super(false);
        this.id = id;
    }

    private void run() {
        long lastTime = System.currentTimeMillis();
        for (;;) {
            try {
                nioEventLoop.select();
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastTime > 1000) {
                    lastTime = currentTime;
                    checkTimeout(currentTime);
                }
                if (isClosed())
                    break;
                if (!registerConnectTasks.isEmpty()) {
                    for (AsyncTask task : registerConnectTasks) {
                        task.run();
                        registerConnectTasks.remove(task);
                    }
                }
                nioEventLoop.write();
                nioEventLoop.handleSelectedKeys();
                if (isClosed())
                    break;
            } catch (Throwable e) {
                if (nioEventLoop != null) // 如果为null，说明关闭了
                    logger.warn("nio client run exception: " + e.getMessage(), e);
            }
        }
    }

    @Override
    protected synchronized void openInternal(Map<String, String> config) {
        if (nioEventLoop == null) {
            // 默认1秒
            long loopInterval = MapUtils.getLong(config, "client_nio_event_loop_interval", 1000);
            nioEventLoop = new NioEventLoop(config, loopInterval, false);
            nioEventLoop.setOwner(this);
            nioEventLoop.setNetClient(this);
            ThreadUtils.start("ClientNioEventLoopService-" + id, () -> {
                NioClient.this.run();
            });
        }
    }

    @Override
    protected synchronized void closeInternal() {
        if (nioEventLoop != null) {
            nioEventLoop.close();
            nioEventLoop = null;
        }
    }

    @Override
    protected NetEventLoop getNetEventLoop() {
        return nioEventLoop;
    }

    @Override
    protected void registerConnectOperation(SocketChannel channel, ClientAttachment attachment,
            AsyncCallback<AsyncConnection> ac) {
        // jdk 1.8不能在另一个线程中注册
        // 当nio-event-loop线程执行selector.select被阻塞时，代码内部依然会占用publicKeys锁，
        // 而另一个线程执行channel.register时，内部也会去要publicKeys锁，从而导致也被阻塞，
        registerConnectTasks.add(() -> {
            try {
                channel.register(nioEventLoop.getSelector(), SelectionKey.OP_CONNECT, attachment);
            } catch (ClosedChannelException e) {
                nioEventLoop.closeChannel(channel);
                ac.setAsyncResult(e);
            }
        });
    }

    @Override
    public boolean isThreadSafe() {
        return false;
    }
}
