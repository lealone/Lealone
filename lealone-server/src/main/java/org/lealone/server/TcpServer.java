/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;

import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.NetUtils;
import org.lealone.db.Constants;
import org.lealone.net.AsyncConnection;
import org.lealone.net.CommandHandler;

/**
 * The TCP server implements the native database server protocol.
 * It supports multiple client connections to multiple databases(many to many). 
 * The same database may be opened by multiple clients.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServer implements ProtocolServer {

    private static Vertx vertx;

    private final CommandHandler commandHandler = new CommandHandler();
    private String listenAddress = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    private String baseDir;

    private boolean ssl;
    private boolean allowOthers;
    private boolean isDaemon;
    private boolean ifExists;
    private Integer blockedThreadCheckInterval;
    private NetServer server;
    private boolean stop;

    @Override
    public void init(Map<String, String> config) { // TODO 对于不支持的参数直接报错
        if (config.containsKey("listen_address"))
            listenAddress = config.get("listen_address");
        if (config.containsKey("listen_port"))
            port = Integer.parseInt(config.get("listen_port"));

        if (config.containsKey("blocked_thread_check_interval"))
            blockedThreadCheckInterval = Integer.parseInt(config.get("blocked_thread_check_interval"));

        baseDir = config.get("base_dir");

        ssl = Boolean.parseBoolean(config.get("ssl"));
        allowOthers = Boolean.parseBoolean(config.get("allow_others"));
        isDaemon = Boolean.parseBoolean(config.get("daemon"));
        ifExists = Boolean.parseBoolean(config.get("if_exists"));
    }

    @Override
    public void start() {
        run();
    }

    @Override
    public synchronized void run() {
        synchronized (TcpServer.class) {
            if (vertx == null) {
                VertxOptions opt = new VertxOptions();
                if (blockedThreadCheckInterval != null) {
                    if (blockedThreadCheckInterval <= 0)
                        blockedThreadCheckInterval = Integer.MAX_VALUE;

                    opt.setBlockedThreadCheckInterval(blockedThreadCheckInterval);
                }
                vertx = Vertx.vertx(opt);
            }
        }
        server = vertx.createNetServer();
        server.connectHandler(socket -> {
            if (TcpServer.this.allow(socket)) {
                AsyncConnection ac = new AsyncConnection(socket);
                ac.setBaseDir(TcpServer.this.baseDir);
                ac.setIfExists(TcpServer.this.ifExists);
                CommandHandler.addConnection(ac);
                socket.handler(ac);
                socket.closeHandler(v -> {
                    CommandHandler.removeConnection(ac);
                });
            } else {
                // TODO
                // should support a list of allowed databases
                // and a list of allowed clients
                socket.close();
                throw DbException.get(ErrorCode.REMOTE_CONNECTION_NOT_ALLOWED);
            }
        });

        CountDownLatch latch = new CountDownLatch(1);
        server.listen(port, listenAddress, res -> {
            if (res.succeeded()) {
                latch.countDown();
            } else {
                throw DbException.convert(res.cause());
            }
        });

        commandHandler.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public synchronized void stop() {
        if (stop)
            return;

        stop = true;
        CountDownLatch latch = new CountDownLatch(1);
        server.close(v -> {
            latch.countDown();
        });

        commandHandler.end();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public synchronized boolean isRunning(boolean traceError) {
        if (stop) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public String getURL() {
        return (ssl ? "ssl" : "tcp") + "://" + getListenAddress() + ":" + port;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getListenAddress() {
        return listenAddress;
    }

    @Override
    public String getName() {
        return "TCP Server";
    }

    @Override
    public String getType() {
        return "TCP";
    }

    @Override
    public boolean getAllowOthers() {
        return allowOthers;
    }

    @Override
    public boolean isDaemon() {
        return isDaemon;
    }

    /**
     * Check if this socket may connect to this server.
     * Remote connections are allowed if the flag allowOthers is set.
     *
     * @param socket the socket
     * @return true if this client may connect
     */
    private boolean allow(NetSocket socket) {
        if (allowOthers) {
            return true;
        }
        try {
            String test = socket.remoteAddress().host();
            InetAddress localhost = InetAddress.getLocalHost();
            // localhost.getCanonicalHostName() is very very slow
            String host = localhost.getHostAddress();
            if (test.equals(host)) {
                return true;
            }

            for (InetAddress addr : InetAddress.getAllByName(host)) {
                if (test.equals(addr)) {
                    return true;
                }
            }
            return false;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    /**
     * Get the configured base directory.
     *
     * @return the base directory
     */
    String getBaseDir() {
        return baseDir;
    }

    boolean getIfExists() {
        return ifExists;
    }

}
