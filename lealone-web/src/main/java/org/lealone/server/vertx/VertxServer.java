/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.vertx;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.MapUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.SysProperties;
import org.lealone.server.ProtocolServerBase;
import org.lealone.server.http.HttpRouter;
import org.lealone.server.http.HttpServer;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class VertxServer extends ProtocolServerBase implements HttpServer {

    private static final Logger logger = LoggerFactory.getLogger(VertxServer.class);

    public static final int DEFAULT_HTTP_PORT = 8080;

    private String webRoot;
    private String jdbcUrl;
    private String apiPath;
    private Vertx vertx;
    private io.vertx.core.http.HttpServer vertxHttpServer;

    private boolean inited;

    public VertxServer() {
        config = new HashMap<>();
    }

    public Vertx getVertx() {
        return vertx;
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return VertxServerEngine.NAME;
    }

    @Override
    public String getWebRoot() {
        return webRoot;
    }

    @Override
    public void setWebRoot(String webRoot) {
        this.webRoot = webRoot;
        config.put("web_root", webRoot);
    }

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        config.put("jdbc_url", jdbcUrl);
        System.setProperty(org.lealone.db.Constants.JDBC_URL_KEY, jdbcUrl);
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public void setHost(String host) {
        this.host = host;
        config.put("host", host);
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public void setPort(int port) {
        this.port = port;
        config.put("port", String.valueOf(port));
    }

    @Override
    public synchronized void init(Map<String, String> config) {
        if (inited)
            return;
        config = new CaseInsensitiveMap<>(config);
        config.putAll(this.config);
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(DEFAULT_HTTP_PORT));

        String baseDir = config.get("base_dir");
        if (baseDir == null) {
            baseDir = "./target/db_base_dir";
            config.put("base_dir", baseDir);
        }
        SysProperties.setBaseDir(baseDir);

        webRoot = config.get("web_root");
        apiPath = config.get("api_path");

        super.init(config);
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
        inited = true;
    }

    @Override
    public synchronized void start() {
        if (isStarted())
            return;
        if (!inited) {
            init(new HashMap<>());
        }
        startVertxHttpServer();
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();

        if (vertxHttpServer != null) {
            vertxHttpServer.close();
            vertxHttpServer = null;
        }
        if (vertx != null) {
            vertx.close();
            vertx = null;
        }
    }

    private void startVertxHttpServer() {
        if (apiPath == null) {
            apiPath = "/_lealone_sockjs_/*";
            config.put("api_path", apiPath);
        }
        final String path = apiPath;
        VertxOptions opt = new VertxOptions();
        long interval = MapUtils.getLong(config, "blocked_thread_check_interval", Integer.MAX_VALUE);
        opt.setBlockedThreadCheckInterval(interval);
        vertx = Vertx.vertx(opt);
        vertxHttpServer = vertx.createHttpServer();
        HttpRouter router;
        String routerStr = config.get("router");
        if (routerStr != null) {
            try {
                router = org.lealone.common.util.Utils.newInstance(routerStr);
            } catch (Exception e) {
                throw new ConfigException("Failed to load router: " + routerStr, e);
            }
        } else {
            router = new VertxRouter();
        }
        router.init(this, config);
        CountDownLatch latch = new CountDownLatch(1);
        vertxHttpServer.requestHandler(((VertxRouter) router).router::handle).listen(port, host, res -> {
            if (res.succeeded()) {
                logger.info("Web root: " + webRoot);
                logger.info("Sockjs path: " + path);
                // logger.info("HttpServer is now listening on port: " + vertxHttpServer.actualPort());
            } else {
                logger.error("Failed to bind " + port + " port!", res.cause());
            }
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
        }
    }
}
