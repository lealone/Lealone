/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http.jdk;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.AsyncConnection;
import com.lealone.net.WritableChannel;
import com.lealone.server.AsyncServer;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpHandler;

public class JdkHttpServer extends AsyncServer<AsyncConnection> {

    private static final Logger logger = LoggerFactory.getLogger(JdkHttpServer.class);

    private Map<String, String> config = new HashMap<>();
    private String webRoot;
    private String jdbcUrl;

    private com.sun.net.httpserver.HttpServer jdkServer;
    private boolean inited;

    @Override
    public String getType() {
        return JdkHttpServerEngine.NAME;
    }

    public String getWebRoot() {
        return webRoot;
    }

    public void setWebRoot(String webRoot) {
        this.webRoot = webRoot;
        config.put("web_root", webRoot);
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        config.put("jdbc_url", jdbcUrl);
        System.setProperty(com.lealone.db.Constants.JDBC_URL_KEY, jdbcUrl);
    }

    @Override
    public String getHost() {
        return super.getHost();
    }

    public void setHost(String host) {
        config.put("host", host);
    }

    @Override
    public int getPort() {
        return super.getPort();
    }

    public void setPort(int port) {
        config.put("port", String.valueOf(port));
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (inited)
            return;
        config = new CaseInsensitiveMap<>(config);
        config.putAll(this.config);
        String url = config.get("jdbc_url");
        if (url != null) {
            if (this.jdbcUrl == null)
                setJdbcUrl(url);
            ConnectionInfo ci = new ConnectionInfo(url);
            if (!config.containsKey("default_database"))
                config.put("default_database", ci.getDatabaseName());
            if (!config.containsKey("default_schema"))
                config.put("default_schema", "public");
        }
        super.init(config);
        // webRoot = MapUtils.getString(config, "web_root", "./web");
        // File webRootDir = new File(webRoot);
        // // 如果没有指定web_root参数也没有web目录就把docBase变成work目录
        // if (!webRootDir.exists()) {
        // webRootDir = new File(getBaseDir(), "work");
        // if (!webRootDir.exists())
        // webRootDir.mkdirs();
        // webRoot = webRootDir.getAbsolutePath();
        // }
        try {
            jdkServer = com.sun.net.httpserver.HttpServer
                    .create(new InetSocketAddress(getHost(), getPort()), 0);
            // Field f = jdkServer.getClass().getDeclaredField("server");
            // if (f != null) {
            // f.setAccessible(true);
            // f = f.get(jdkServer).getClass().getDeclaredField("timer");
            // if (f != null)
            // ((java.util.Timer) f.get(jdkServer)).cancel();
            // }
            JdkHttpRouter router;
            String routerStr = config.get("router");
            if (routerStr != null) {
                try {
                    router = com.lealone.common.util.Utils.newInstance(routerStr);
                } catch (Exception e) {
                    throw new ConfigException("Failed to load router: " + routerStr, e);
                }
            } else {
                router = new JdkHttpRouter();
            }
            router.init(this, config);
        } catch (Exception e) {
            logger.error("Failed to init jdk http server", e);
        }

        inited = true;
        this.config = null;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        if (!inited) {
            init(new HashMap<>());
        }
        // super.start();
        try {
            jdkServer.start();
        } catch (Exception e) {
            logger.error("Failed to start jdk http server", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        if (jdkServer != null) {
            try {
                jdkServer.stop(0);
            } catch (Exception e) {
                logger.error("Failed to stop jdk http server", e);
            }
            jdkServer = null;
        }
    }

    @Override
    protected int getDefaultPort() {
        return JdkHttpServerEngine.DEFAULT_PORT;
    }

    @Override
    protected AsyncConnection createConnection(WritableChannel channel, Scheduler scheduler) {
        return null;
    }

    public HttpContext createContext(String path, HttpHandler handler) {
        return jdkServer.createContext(path, handler);
    }
}
