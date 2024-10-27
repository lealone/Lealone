/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net.nio;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;

import com.lealone.db.async.AsyncCallback;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.AsyncConnection;
import com.lealone.net.AsyncConnectionManager;
import com.lealone.net.AsyncConnectionPool;
import com.lealone.net.NetClientBase;
import com.lealone.net.NetEventLoop;
import com.lealone.net.NetNode;

public class NioNetClient extends NetClientBase {

    public NioNetClient() {
        super(false);
    }

    public NioNetClient(boolean isThreadSafe) {
        super(isThreadSafe);
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
