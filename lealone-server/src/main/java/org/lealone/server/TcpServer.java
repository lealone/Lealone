/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.NetUtils;
import org.lealone.db.Constants;

/**
 * The TCP server implements the native database server protocol.
 * It supports multiple client connections to multiple databases(many to many). 
 * The same database may be opened by multiple clients.
 * 
 * @author H2 Group
 * @author zhh
 */
public class TcpServer implements ProtocolServer {

    private final Set<TcpServerThread> running = new ConcurrentSkipListSet<>();

    private ServerSocket serverSocket;
    private Thread listenerThread;

    private String listenAddress = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    private String baseDir;

    private boolean trace;
    private boolean ssl;
    private boolean allowOthers;
    private boolean isDaemon;
    private boolean ifExists;

    private boolean stop;

    @Override
    public void init(Map<String, String> config) { // TODO 对于不支持的参数直接报错
        if (config.containsKey("listen_address"))
            listenAddress = config.get("listen_address");
        if (config.containsKey("listen_port"))
            port = Integer.parseInt(config.get("listen_port"));

        baseDir = config.get("base_dir");

        trace = Boolean.parseBoolean(config.get("trace"));
        ssl = Boolean.parseBoolean(config.get("ssl"));
        allowOthers = Boolean.parseBoolean(config.get("allow_others"));
        isDaemon = Boolean.parseBoolean(config.get("daemon"));
        ifExists = Boolean.parseBoolean(config.get("if_exists"));
    }

    @Override
    public synchronized void start() {
        serverSocket = NetUtils.createServerSocket(listenAddress, port, ssl);

        String name = getName() + " (" + getURL() + ")";
        Thread t = new Thread(this, name);
        t.setDaemon(isDaemon());
        t.start();
    }

    @Override
    public void run() {
        try {
            listen();
        } catch (Exception e) {
            DbException.traceThrowable(e);
        }
    }

    private void listen() {
        listenerThread = Thread.currentThread();
        String threadName = listenerThread.getName();
        try {
            while (!stop) {
                Socket s = serverSocket.accept();
                if (stop)
                    break;
                TcpServerThread t = new TcpServerThread(s, this);
                t.setName(threadName + " thread-" + s.getPort());
                t.setDaemon(isDaemon);
                t.start();
                running.add(t);
            }
            serverSocket = NetUtils.closeSilently(serverSocket);
        } catch (Exception e) {
            if (!stop) {
                DbException.traceThrowable(e);
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (stop)
            return;

        stop = true;
        try {
            Socket s = NetUtils.createLoopbackSocket(port, false);
            s.close();
        } catch (Exception e) {
            serverSocket = NetUtils.closeSilently(serverSocket);
        }

        if (listenerThread != null) {
            try {
                listenerThread.join(1000);
            } catch (InterruptedException e) {
                DbException.traceThrowable(e);
            }
        }
        // TODO server: using a boolean 'now' argument? a timeout?
        for (TcpServerThread c : running) {
            c.close();
            try {
                c.join(100);
            } catch (Exception e) {
                DbException.traceThrowable(e);
            }
        }
        running.clear();
    }

    @Override
    public synchronized boolean isRunning(boolean traceError) {
        if (serverSocket == null) {
            return false;
        }
        try {
            Socket s = NetUtils.createLoopbackSocket(port, ssl);
            s.close();
            return true;
        } catch (Exception e) {
            if (traceError) {
                traceError(e);
            }
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
    boolean allow(Socket socket) {
        if (allowOthers) {
            return true;
        }
        try {
            return NetUtils.isLocalAddress(socket);
        } catch (UnknownHostException e) {
            traceError(e);
            return false;
        }
    }

    /**
     * Remove a thread from the list.
     *
     * @param t the thread to remove
     */
    void remove(TcpServerThread t) {
        running.remove(t);
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

    boolean isTraceEnabled() {
        return trace;
    }

    /**
     * Print a message if the trace flag is enabled.
     *
     * @param s the message
     */
    void trace(String s) {
        if (trace) {
            System.out.println(s);
        }
    }

    /**
     * Print a stack trace if the trace flag is enabled.
     *
     * @param e the exception
     */
    void traceError(Throwable e) {
        if (trace) {
            e.printStackTrace();
        }
    }

    /**
     * Cancel a running statement.
     *
     * @param sessionId the session id
     * @param statementId the statement id
     */
    void cancelStatement(String sessionId, int statementId) {
        for (TcpServerThread c : running) {
            c.cancelStatement(sessionId, statementId);
        }
    }

}
