/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;

import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;

public abstract class ProtocolServerBase implements ProtocolServer {

    public static final int DEFAULT_SESSION_TIMEOUT = 15 * 60 * 1000; // 如果session在15分钟内不活跃就会超时

    protected Map<String, String> config;
    protected String host = Constants.DEFAULT_HOST;
    protected int port;

    protected String baseDir;
    protected String name;

    protected boolean ssl;
    protected boolean allowOthers;
    protected boolean isDaemon;
    protected boolean stopped;
    protected boolean started;

    // 如果allowOthers为false，那么可以指定具体的白名单，只有在白名单中的客户端才可以连进来
    protected HashSet<String> whiteList;
    protected ServerEncryptionOptions serverEncryptionOptions;
    protected int sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    protected ProtocolServerBase() {
    }

    protected ProtocolServerBase(int port) {
        this.port = port;
    }

    @Override
    public void init(Map<String, String> config) { // TODO 对于不支持的参数直接报错
        this.config = config;
        if (config.containsKey("host"))
            host = config.get("host");
        if (config.containsKey("port"))
            port = Integer.parseInt(config.get("port"));

        baseDir = config.get("base_dir");
        name = config.get("name");

        ssl = Boolean.parseBoolean(config.get("ssl"));
        allowOthers = Boolean.parseBoolean(config.get("allow_others"));
        isDaemon = Boolean.parseBoolean(config.get("daemon"));

        if (config.containsKey("white_list")) {
            String[] hosts = config.get("white_list").split(",");
            whiteList = new HashSet<>(hosts.length);
            for (String host : hosts) {
                whiteList.add(host);
            }
        }
        if (config.containsKey("session_timeout"))
            sessionTimeout = Integer.parseInt(config.get("session_timeout"));
    }

    @Override
    public synchronized void start() {
        started = true;
        stopped = false;
    }

    @Override
    public synchronized void stop() {
        started = false;
        stopped = true;
    }

    @Override
    public synchronized boolean isRunning(boolean traceError) {
        return started && !stopped;
    }

    @Override
    public String getURL() {
        return (ssl ? "ssl" : getType()) + "://" + getHost() + ":" + port;
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
        return name != null ? name : getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return getName();
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
    * Get the configured base directory.
    *
    * @return the base directory
    */
    @Override
    public String getBaseDir() {
        return baseDir;
    }

    @Override
    public void setServerEncryptionOptions(ServerEncryptionOptions options) {
        this.serverEncryptionOptions = options;
    }

    @Override
    public ServerEncryptionOptions getServerEncryptionOptions() {
        return serverEncryptionOptions;
    }

    @Override
    public boolean isSSL() {
        return ssl;
    }

    @Override
    public Map<String, String> getConfig() {
        return config;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    @Override
    public boolean isStopped() {
        return stopped;
    }

    @Override
    public boolean allow(String testHost) {
        if (allowOthers) {
            return true;
        }
        try {
            if (whiteList != null && whiteList.contains(testHost))
                return true;

            InetAddress localhost = InetAddress.getLocalHost();
            // localhost.getCanonicalHostName() is very very slow
            String host = localhost.getHostAddress();
            if (testHost.equals(host)) {
                return true;
            }

            for (InetAddress addr : InetAddress.getAllByName(host)) {
                if (testHost.equals(addr.getHostAddress())) {
                    return true;
                }
            }
            return false;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    @Override
    public int getSessionTimeout() {
        return sessionTimeout;
    }
}
