/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net.nio;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.lealone.db.async.AsyncCallback;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.AsyncConnectionPool;
import org.lealone.net.NetClientBase;
import org.lealone.net.NetEventLoop;
import org.lealone.net.NetNode;

class NioClient extends NetClientBase {

    NioClient() {
        super(false);
    }

    @Override
    protected void createConnectionInternal(Map<String, String> config, NetNode node, //
            AsyncConnectionManager connectionManager, AsyncCallback<AsyncConnection> ac,
            Scheduler scheduler) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        SocketChannel channel = null;
        NetEventLoop eventLoop = (NetEventLoop) scheduler.getNetEventLoop();
        try {
            channel = SocketChannel.open();
            channel.configureBlocking(false);
            initSocket(channel.socket(), config);

            ClientAttachment attachment = new ClientAttachment();
            attachment.connectionManager = connectionManager;
            attachment.inetSocketAddress = inetSocketAddress;
            attachment.ac = ac;
            attachment.maxSharedSize = AsyncConnectionPool.getMaxSharedSize(config);

            channel.register(eventLoop.getSelector(), SelectionKey.OP_CONNECT, attachment);
            channel.connect(inetSocketAddress);
            // 如果前面已经在执行事件循环，此时就不能再次进入事件循环
            // 否则两次删除SelectionKey会出现java.util.ConcurrentModificationException
            if (!eventLoop.isInLoop()) {
                if (eventLoop.getSelector().selectNow() > 0) {
                    eventLoop.handleSelectedKeys();
                }
            }
        } catch (Exception e) {
            eventLoop.closeChannel(channel);
            ac.setAsyncResult(e);
        }
    }
}
