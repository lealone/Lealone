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
    private final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean opened = new AtomicBoolean(false);

    public NetClientBase() {
    }

    protected abstract void openInternal(Map<String, String> config);

    protected abstract void closeInternal();

    protected abstract void createConnectionInternal(NetNode node, AsyncConnectionManager connectionManager,
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
        AsyncConnection asyncConnection = getConnection(inetSocketAddress);
        if (asyncConnection == null) {
            AsyncCallback<AsyncConnection> ac = new AsyncCallback<>();
            createConnectionInternal(node, connectionManager, ac);
            return ac;
        } else {
            return Future.succeededFuture(asyncConnection);
        }
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress) {
        // checkClosed(); //不做检查
        AsyncConnection conn = asyncConnections.remove(inetSocketAddress);
        if (conn != null && !conn.isClosed())
            conn.close();
    }

    protected AsyncConnection getConnection(InetSocketAddress inetSocketAddress) {
        checkClosed();
        return asyncConnections.get(inetSocketAddress);
    }

    protected AsyncConnection addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn) {
        checkClosed();
        // 每个InetSocketAddress只保留一个连接，如果已经存在就复用老的，然后关闭新打开的
        AsyncConnection old = asyncConnections.putIfAbsent(inetSocketAddress, conn);
        if (old != null) {
            conn.close();
            conn = old;
        }
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
        for (AsyncConnection conn : asyncConnections.values()) {
            try {
                conn.close();
            } catch (Throwable e) {
            }
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
        for (AsyncConnection conn : asyncConnections.values()) {
            conn.checkTimeout(currentTime);
        }
    }
}
