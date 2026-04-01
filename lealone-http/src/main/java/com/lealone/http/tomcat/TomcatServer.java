/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.CoyoteAdapter;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.startup.Tomcat;
import org.apache.coyote.Processor;
import org.apache.tomcat.util.net.NioChannel;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.MapUtils;
import com.lealone.db.ConnectionInfo;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.WritableChannel;
import com.lealone.server.AsyncServer;
import com.lealone.service.http.HttpRouter;
import com.lealone.service.http.HttpServer;

import jakarta.servlet.Servlet;

public class TomcatServer extends AsyncServer<TomcatServerConnection> implements HttpServer {

    private static final Logger logger = LoggerFactory.getLogger(TomcatServer.class);

    public static final int DEFAULT_PORT = 8080;

    private Map<String, String> config = new HashMap<>();
    private String webRoot;
    private String jdbcUrl;

    private String contextPath;
    private Tomcat tomcat;
    private Context ctx;

    public Tomcat getTomcat() {
        return tomcat;
    }

    public Context getContext() {
        return ctx;
    }

    private TomcatHttp11NioProtocol protocolHandler;
    private LinkedList<Processor>[] recycledProcessors;
    private LinkedList<NioChannel>[] nioChannels;

    private boolean inited;

    public TomcatHttp11NioProtocol getProtocolHandler() {
        return protocolHandler;
    }

    public LinkedList<Processor> getRecycledProcessors(int index) {
        return recycledProcessors[index];
    }

    public LinkedList<NioChannel> getNioChannels(int index) {
        return nioChannels[index];
    }

    @Override
    public String getType() {
        return TomcatServerEngine.NAME;
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
        System.setProperty(com.lealone.db.Constants.JDBC_URL_KEY, jdbcUrl);
    }

    @Override
    public String getHost() {
        return super.getHost();
    }

    @Override
    public void setHost(String host) {
        config.put("host", host);
    }

    @Override
    public int getPort() {
        return super.getPort();
    }

    @Override
    public void setPort(int port) {
        config.put("port", String.valueOf(port));
    }

    @Override
    @SuppressWarnings("unchecked")
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
        // 跟spring框架集成时在它那边初始化tomcat
        boolean initTomcat = MapUtils.getBoolean(config, "init_tomcat", true);
        super.init(config);
        int schedulerCount = MapUtils.getSchedulerCount(config);
        recycledProcessors = new LinkedList[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            recycledProcessors[i] = new LinkedList<>();
        }
        nioChannels = new LinkedList[schedulerCount];
        for (int i = 0; i < schedulerCount; i++) {
            nioChannels[i] = new LinkedList<>();
        }

        contextPath = MapUtils.getString(config, "context_path", "");
        webRoot = MapUtils.getString(config, "web_root", "./web");
        File webRootDir = new File(webRoot);
        if (initTomcat) {
            // 如果没有指定web_root参数也没有web目录就把docBase变成work目录
            if (!webRootDir.exists()) {
                webRootDir = new File(getBaseDir(), "work");
                if (!webRootDir.exists())
                    webRootDir.mkdirs();
            }
        }
        try {
            tomcat = new Tomcat();
            tomcat.setBaseDir(getBaseDir());
            if (initTomcat) {
                tomcat.setHostname(getHost());
                tomcat.setPort(getPort());
                ctx = tomcat.addContext(contextPath, webRootDir.getCanonicalPath());
            }

            ((StandardServer) tomcat.getServer()).setPeriodicEventDelay(0);
            tomcat.getEngine().setBackgroundProcessorDelay(0);

            protocolHandler = new TomcatHttp11NioProtocol();
            protocolHandler.setPort(getPort());
            protocolHandler.setMaxKeepAliveRequests(100 * 10000);
            // 不禁用时如果js文件大了会出现net::err_content_length_mismatch 200
            protocolHandler.setUseSendfile(false);
            TomcatConnector connector = new TomcatConnector(protocolHandler);
            CoyoteAdapter adapter = new CoyoteAdapter(connector);
            protocolHandler.setAdapter(adapter);
            tomcat.setConnector(connector);

            HttpRouter router;
            String routerStr = config.get("router");
            if (routerStr != null) {
                try {
                    router = com.lealone.common.util.Utils.newInstance(routerStr);
                } catch (Exception e) {
                    throw new ConfigException("Failed to load router: " + routerStr, e);
                }
            } else {
                router = new TomcatRouter();
            }
            if (initTomcat) {
                router.init(this, config);
                tomcat.init();
            }
        } catch (Exception e) {
            logger.error("Failed to init tomcat", e);
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
        super.start();
        try {
            tomcat.start();
        } catch (LifecycleException e) {
            logger.error("Failed to start tomcat", e);
        }
        // ShutdownHookUtils.addShutdownHook(tomcat, () -> {
        // try {
        // tomcat.destroy();
        // } catch (LifecycleException e) {
        // logger.error("Failed to destroy tomcat", e);
        // }
        // });
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();
        for (int i = 0; i < nioChannels.length; i++) {
            if (nioChannels[i].isEmpty())
                continue;
            NioChannel socket;
            while ((socket = nioChannels[i].pop()) != null) {
                socket.free();
            }
        }
        nioChannels = null;

        if (tomcat != null) {
            try {
                tomcat.stop();
            } catch (LifecycleException e) {
                logger.error("Failed to stop tomcat", e);
            }
            tomcat = null;
        }
        // executorService.shutdown();
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected TomcatServerConnection createConnection(WritableChannel channel, Scheduler scheduler) {
        return new TomcatServerConnection(this, channel, scheduler);
    }

    public void addServlet(String servletName, Servlet servlet) {
        Tomcat.addServlet(ctx, servletName, servlet);
    }

    public void addServletMappingDecoded(String pattern, String name) {
        ctx.addServletMappingDecoded(pattern, name);
    }

    // private ExecutorService executorService = Executors
    // .newThreadPerTaskExecutor(Thread.ofVirtual().name("lealone-http-vt-", 1).factory());

    public void submit(Runnable task) {
        // executorService.submit(task);
        task.run();
    }
}
