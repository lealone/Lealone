/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class NetClientBase implements NetClient {

    // 使用InetSocketAddress为key而不是字符串，是因为像localhost和127.0.0.1这两种不同格式实际都是同一个意思，
    // 如果用字符串，就会产生两条AsyncConnection，这是没必要的。
    private final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean opened = new AtomicBoolean(false);

    public NetClientBase() {
    }

    protected abstract void openInternal(Map<String, String> config);

    protected abstract void createConnectionInternal(NetEndpoint endpoint, AsyncConnectionManager connectionManager,
            CountDownLatch latch) throws Throwable;

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint) {
        // checkClosed(); //创建连接时不检查关闭状态，这样允许重用NetClient实例，如果之前的实例关闭了，重新打开即可
        return createConnection(config, endpoint, null);
    }

    private synchronized void open(Map<String, String> config) {
        if (opened.get())
            return;
        openInternal(config);
        opened.set(true);
    }

    @Override
    public AsyncConnection createConnection(Map<String, String> config, NetEndpoint endpoint,
            AsyncConnectionManager connectionManager) {
        if (!opened.get()) {
            open(config);
        }
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        AsyncConnection asyncConnection = getConnection(inetSocketAddress);
        if (asyncConnection == null) {
            synchronized (this) {
                asyncConnection = getConnection(inetSocketAddress);
                if (asyncConnection == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    try {
                        createConnectionInternal(endpoint, connectionManager, latch);
                        latch.await();
                    } catch (Throwable e) {
                        throw new RuntimeException("Cannot connect to " + inetSocketAddress, e);
                    }
                    asyncConnection = getConnection(inetSocketAddress);
                    if (asyncConnection == null) {
                        throw new RuntimeException("Cannot connect to " + inetSocketAddress);
                    }
                }
            }
        }
        return asyncConnection;
    }

    @Override
    public void removeConnection(InetSocketAddress inetSocketAddress) {
        checkClosed();
        AsyncConnection conn = asyncConnections.remove(inetSocketAddress);
        if (conn != null && !conn.isClosed())
            conn.close();
    }

    protected AsyncConnection getConnection(InetSocketAddress inetSocketAddress) {
        checkClosed();
        return asyncConnections.get(inetSocketAddress);
    }

    protected void addConnection(InetSocketAddress inetSocketAddress, AsyncConnection conn) {
        checkClosed();
        asyncConnections.put(inetSocketAddress, conn);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true))
            return;
        opened.set(false);
        for (AsyncConnection conn : asyncConnections.values()) {
            try {
                conn.close();
            } catch (Throwable e) {
            }
        }
        asyncConnections.clear();
        closeInternal();
    }

    protected void checkClosed() {
        if (isClosed()) {
            throw new RuntimeException("NetClient is closed");
        }
    }

    protected void closeInternal() {
    }
}
