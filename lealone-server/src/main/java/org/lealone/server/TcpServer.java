/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;
import org.lealone.net.AsyncConnection;
import org.lealone.net.CommandHandler;
import org.lealone.net.NetEndpoint;
import org.lealone.net.NetFactory;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;

/**
 * The TCP server implements the native database server protocol.
 * It supports multiple client connections to multiple databases(many to many). 
 * The same database may be opened by multiple clients.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServer implements ProtocolServer {

    private static final Logger logger = LoggerFactory.getLogger(TcpServer.class);
    private static Vertx vertx;

    private String host = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    private String baseDir;

    private boolean ssl;
    private boolean allowOthers;
    private boolean isDaemon;
    private NetServer server;
    private boolean stop;
    private ServerEncryptionOptions options;

    @Override
    public void init(Map<String, String> config) { // TODO 对于不支持的参数直接报错
        if (config.containsKey("host"))
            host = config.get("host");
        if (config.containsKey("port"))
            port = Integer.parseInt(config.get("port"));

        baseDir = config.get("base_dir");

        ssl = Boolean.parseBoolean(config.get("ssl"));
        allowOthers = Boolean.parseBoolean(config.get("allow_others"));
        isDaemon = Boolean.parseBoolean(config.get("daemon"));

        synchronized (TcpServer.class) {
            if (vertx == null) {
                vertx = NetFactory.getVertx(config);
            }
        }

        NetEndpoint.setLocalTcpEndpoint(host, port);
    }

    @Override
    public synchronized void start() {
        server = NetFactory.createNetServer(vertx, ssl ? options : null);
        server.connectHandler(socket -> {
            if (TcpServer.this.allow(socket)) {
                AsyncConnection ac = new AsyncConnection(socket, true);
                ac.setBaseDir(TcpServer.this.baseDir);
                CommandHandler.addConnection(ac);
                socket.handler(ac);
                String msg = "RemoteAddress " + socket.remoteAddress() + " ";
                socket.closeHandler(v -> {
                    logger.info(msg + "closed");
                    CommandHandler.removeConnection(ac);
                });
                socket.exceptionHandler(v -> {
                    logger.warn(msg + "exception: " + v.getMessage());
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
        server.listen(port, host, res -> {
            if (res.succeeded()) {
                latch.countDown();
            } else {
                throw DbException.convert(res.cause());
            }
        });

        CommandHandler.startCommandHandlers();
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

        CommandHandler.stopCommandHandlers();
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
        return true;
    }

    @Override
    public String getURL() {
        return (ssl ? "ssl" : "tcp") + "://" + getHost() + ":" + port;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
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

    @Override
    public void setServerEncryptionOptions(ServerEncryptionOptions options) {
        this.options = options;
    }

}
