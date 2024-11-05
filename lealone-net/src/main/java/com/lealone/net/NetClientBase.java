/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.util.MapUtils;
import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.async.AsyncCallback;
import com.lealone.db.async.Future;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.nio.NioAttachment;

public abstract class NetClientBase implements NetClient {

    public NetClientBase() {
        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
    }

    // 使用InetSocketAddress为key而不是字符串，是因为像localhost和127.0.0.1这两种不同格式实际都是同一个意思，
    // 如果用字符串，就会产生两条AsyncConnection，这是没必要的。
    private final Map<InetSocketAddress, AsyncConnectionPool> asyncConnections = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected abstract void createConnectionInternal(Map<String, String> config, NetNode node, //
            AsyncConnectionManager connectionManager, AsyncCallback<AsyncConnection> ac,
            Scheduler scheduler);

    @Override
    public Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, Scheduler scheduler) {
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        AsyncConnection asyncConnection = getConnection(config, inetSocketAddress);
        if (asyncConnection == null) {
            AsyncCallback<AsyncConnection> ac = AsyncCallback.create(true);
            createConnectionInternal(config, node, connectionManager, ac, scheduler);
            return ac;
        } else {
            return Future.succeededFuture(asyncConnection);
        }
    }

    @Override
    public AsyncConnection getConnection(InetSocketAddress inetSocketAddress) {
        AsyncConnectionPool pool = asyncConnections.get(inetSocketAddress);
        return pool == null ? null : pool.getConnection();
    }

    private AsyncConnection getConnection(Map<String, String> config,
            InetSocketAddress inetSocketAddress) {
        AsyncConnectionPool pool = asyncConnections.get(inetSocketAddress);
        return pool == null ? null : pool.getConnection(config);
    }

    @Override
    public void removeConnection(AsyncConnection conn) {
        if (conn == null)
            return;
        AsyncConnectionPool pool = asyncConnections.get(conn.getInetSocketAddress());
        if (pool != null) {
            pool.removeConnection(conn);
            if (pool.isEmpty()) {
                asyncConnections.remove(conn.getInetSocketAddress());
            }
        }
        if (!conn.isClosed())
            conn.close();
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress) {
        AsyncConnectionPool pool = asyncConnections.remove(inetSocketAddress);
        if (pool != null) {
            pool.close();
        }
    }

    @Override
    public void addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn) {
        checkClosed();
        AsyncConnectionPool pool = asyncConnections.get(inetSocketAddress);
        if (pool == null) {
            pool = new AsyncConnectionPool();
            AsyncConnectionPool old = asyncConnections.putIfAbsent(inetSocketAddress, pool);
            if (old != null)
                pool = old;
        }
        pool.addConnection(conn);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true))
            return;
        for (AsyncConnectionPool pool : asyncConnections.values()) {
            pool.close();
        }
        asyncConnections.clear();
    }

    protected void checkClosed() {
        if (isClosed()) {
            throw new RuntimeException("NetClient is closed");
        }
    }

    @Override
    public void checkTimeout(long currentTime) {
        if (!asyncConnections.isEmpty()) {
            for (AsyncConnectionPool pool : asyncConnections.values()) {
                pool.checkTimeout(currentTime);
            }
        }
    }

    protected void initSocket(Socket socket, Map<String, String> config) throws SocketException {
        int socketRecvBuffer = MapUtils.getInt(config, ConnectionSetting.SOCKET_RECV_BUFFER_SIZE.name(),
                16 * 1024);
        int socketSendBuffer = MapUtils.getInt(config, ConnectionSetting.SOCKET_SEND_BUFFER_SIZE.name(),
                8 * 1024);
        socket.setReceiveBufferSize(socketRecvBuffer);
        socket.setSendBufferSize(socketSendBuffer);
        socket.setTcpNoDelay(true);
        socket.setKeepAlive(true);
        socket.setReuseAddress(true);
    }

    public static class ClientAttachment extends NioAttachment {
        public AsyncConnectionManager connectionManager;
        public InetSocketAddress inetSocketAddress;
        public AsyncCallback<AsyncConnection> ac;
        public int maxSharedSize;
    }
}
