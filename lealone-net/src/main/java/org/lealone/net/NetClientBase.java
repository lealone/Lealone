/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.db.async.AsyncCallback;
import org.lealone.db.async.Future;

public abstract class NetClientBase implements NetClient {

    // 使用InetSocketAddress为key而不是字符串，是因为像localhost和127.0.0.1这两种不同格式实际都是同一个意思，
    // 如果用字符串，就会产生两条AsyncConnection，这是没必要的。
    private final ConcurrentHashMap<InetSocketAddress, AsyncConnectionPool> asyncConnections //
            = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean opened = new AtomicBoolean(false);

    public NetClientBase() {
    }

    protected abstract void openInternal(Map<String, String> config);

    protected abstract void closeInternal();

    protected abstract void createConnectionInternal(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager, int maxSharedSize,
            AsyncCallback<AsyncConnection> ac);

    @Override
    public Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node) {
        return createConnection(config, node, null);
    }

    private synchronized void open(Map<String, String> config) {
        if (opened.get())
            return;
        openInternal(config);
        ShutdownHookUtils.addShutdownHook(this, () -> {
            close();
        });
        opened.set(true);
    }

    @Override
    public Future<AsyncConnection> createConnection(Map<String, String> config, NetNode node,
            AsyncConnectionManager connectionManager) {
        // checkClosed(); //创建连接时不检查关闭状态，这样允许重用NetClient实例，如果之前的实例关闭了，重新打开即可
        if (!opened.get()) {
            open(config);
        }
        InetSocketAddress inetSocketAddress = node.getInetSocketAddress();
        AsyncConnection asyncConnection = getConnection(config, inetSocketAddress);
        if (asyncConnection == null) {
            AsyncCallback<AsyncConnection> ac = new AsyncCallback<>();
            createConnectionInternal(config, node, connectionManager,
                    AsyncConnectionPool.getMaxSharedSize(config), ac);
            return ac;
        } else {
            return Future.succeededFuture(asyncConnection);
        }
    }

    @Override
    public void removeConnection(AsyncConnection conn) {
        // checkClosed(); //不做检查
        if (conn == null)
            return;
        AsyncConnectionPool pool = asyncConnections.get(conn.getInetSocketAddress());
        if (pool != null) {
            pool.removeConnection(conn);
        }
        if (!conn.isClosed())
            conn.close();
    }

    protected AsyncConnection getConnection(Map<String, String> config,
            InetSocketAddress inetSocketAddress) {
        checkClosed();
        AsyncConnectionPool pool = asyncConnections.get(inetSocketAddress);
        return pool == null ? null : pool.getConnection(config);
    }

    protected AsyncConnection addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn) {
        checkClosed();
        AsyncConnectionPool pool = asyncConnections.get(inetSocketAddress);
        if (pool == null) {
            pool = new AsyncConnectionPool();
            AsyncConnectionPool old = asyncConnections.putIfAbsent(inetSocketAddress, pool);
            if (old != null)
                pool = old;
        }
        pool.addConnection(conn);
        return conn;
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
        closeInternal();
        opened.set(false);
    }

    protected void checkClosed() {
        if (isClosed()) {
            throw new RuntimeException("NetClient is closed");
        }
    }

    public void checkTimeout(long currentTime) {
        for (AsyncConnectionPool pool : asyncConnections.values()) {
            pool.checkTimeout(currentTime);
        }
    }
}
