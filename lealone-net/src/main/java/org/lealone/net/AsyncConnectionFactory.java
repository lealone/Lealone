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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.DbException;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;

public class AsyncConnectionFactory {

    // 使用InetSocketAddress为key而不是字符串，是因为像localhost和127.0.0.1这两种不同格式实际都是同一个意思，
    // 如果用字符串，就会产生两条AsyncConnection，这是没必要的。
    private static final ConcurrentHashMap<InetSocketAddress, AsyncConnection> asyncConnections = new ConcurrentHashMap<>();

    private static Vertx vertx;
    private static NetClient client;

    private static void openClient(Properties prop) {
        synchronized (AsyncConnectionFactory.class) {
            if (client == null) {
                vertx = NetFactory.getVertx(prop);
                NetClientOptions options = NetFactory.getNetClientOptions(prop);
                options.setConnectTimeout(10000);
                client = vertx.createNetClient(options);
            }
        }
    }

    private static void closeClient() {
        synchronized (AsyncConnectionFactory.class) {
            if (client != null) {
                client.close();
                NetFactory.closeVertx(vertx); // 不要像这样单独调用: vertx.close();
                client = null;
                vertx = null;
            }
        }
    }

    public static AsyncConnection createConnection(Properties prop, NetEndpoint endpoint) {
        if (client == null) {
            openClient(prop);
        }
        InetSocketAddress inetSocketAddress = endpoint.getInetSocketAddress();
        AsyncConnection asyncConnection = asyncConnections.get(inetSocketAddress);
        if (asyncConnection == null) {
            synchronized (AsyncConnectionFactory.class) {
                asyncConnection = asyncConnections.get(inetSocketAddress);
                if (asyncConnection == null) {
                    CountDownLatch latch = new CountDownLatch(1);
                    client.connect(endpoint.getPort(), endpoint.getHost(), res -> {
                        try {
                            if (res.succeeded()) {
                                NetSocket socket = res.result();
                                AsyncConnection connection = new AsyncConnection(socket, false);
                                connection.setHostAndPort(endpoint.getHostAndPort());
                                connection.setInetSocketAddress(inetSocketAddress);
                                asyncConnections.put(inetSocketAddress, connection);
                                socket.handler(connection);
                            } else {
                                throw DbException.convert(res.cause());
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                    try {
                        latch.await();
                        asyncConnection = asyncConnections.get(inetSocketAddress);
                    } catch (InterruptedException e) {
                        throw DbException.convert(e);
                    }
                }
            }
        }
        return asyncConnection;
    }

    public static void removeConnection(InetSocketAddress inetSocketAddress) {
        asyncConnections.remove(inetSocketAddress);
        if (asyncConnections.isEmpty()) {
            closeClient();
        }
    }
}
