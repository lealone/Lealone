/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http.jdk;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.common.util.IOUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.db.service.ServiceHandler;
import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.SimpleFileServer;

public class JdkHttpRouter {

    // private static final Logger log = LoggerFactory.getLogger(JdkHttpRouter.class);

    protected JdkHttpServer jdkHttpServer;
    protected String webRoot;
    protected String uploadDirectory;
    protected HttpContext httpContext;

    public void init(JdkHttpServer server, Map<String, String> config) {
        jdkHttpServer = server;
        webRoot = config.get("web_root");
        uploadDirectory = config.get("upload_directory");
        if (uploadDirectory == null)
            uploadDirectory = webRoot + "/file_uploads";
        init(config);
    }

    public void init(Map<String, String> config) {
        jdkHttpServer.createContext("/service", new JdkHttpServiceHandler(new ServiceHandler(config)));
        jdkHttpServer.createContext("/agent", new AgentHandler());
        if (webRoot != null)
            httpContext = jdkHttpServer.createContext("/", SimpleFileServer
                    .createFileHandler(Path.of(new java.io.File(webRoot).getAbsolutePath())));
        initFilters(config);
    }

    private void initFilters(Map<String, String> config) {
        for (Entry<String, String> e : config.entrySet()) {
            String key = e.getKey();
            if (key.equalsIgnoreCase("redirect_filter")) {
                RedirectFilter rf = new RedirectFilter();
                for (String redirect : StringUtils.arraySplit(e.getValue(), ',')) {
                    String[] a = StringUtils.arraySplit(redirect, ':');
                    rf.addRule(a[0], a[1]);
                }
                if (httpContext != null)
                    httpContext.getFilters().add(rf);
            } else if (key.equalsIgnoreCase("file_upload_filter")) {
                // for (String urlPattern : StringUtils.arraySplit(e.getValue(), ',')) {
                // addFileUploadFilter(urlPattern);
                // }
            } else if (key.equalsIgnoreCase("filters")) {
                // for (String filterAndUrlPattern : StringUtils.arraySplit(e.getValue(), ';')) {
                // String[] a = StringUtils.arraySplit(filterAndUrlPattern, ':');
                // String filterClassName = a[0];
                // try {
                // for (String urlPattern : StringUtils.arraySplit(a[1], ',')) {
                // HttpFilter filter = Utils.newInstance(filterClassName);
                // addFilter(filter, urlPattern);
                // }
                // } catch (Throwable t) {
                // log.warn("", t);
                // }
                // }
            }
        }
    }

    /**
     * URL 重定向过滤器
     * 支持：精确路径、前缀匹配、301/302 重定向
     */
    private static class RedirectFilter extends Filter {

        // key: 源URL前缀/精确路径 value: 目标重定向地址
        private final Map<String, String> redirectMap = new HashMap<>();
        // 301永久重定向 / 302临时重定向
        private final int statusCode;

        /**
         * 默认 302 临时重定向
         */
        public RedirectFilter() {
            this(302);
        }

        /**
         * 自定义重定向状态码
         * @param statusCode 301 / 302
         */
        public RedirectFilter(int statusCode) {
            this.statusCode = statusCode;
        }

        /**
         * 添加重定向规则：源路径 -> 目标URL
         * @param sourcePath 源路径 如 /old、/api/old
         * @param targetUrl 目标地址 如 /new、https://xxx.com
         */
        public RedirectFilter addRule(String sourcePath, String targetUrl) {
            redirectMap.put(sourcePath, targetUrl);
            return this;
        }

        @Override
        public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
            String path = exchange.getRequestURI().getPath();

            // 匹配前缀规则
            for (Map.Entry<String, String> entry : redirectMap.entrySet()) {
                String source = entry.getKey();
                String target = entry.getValue();
                if (path.equalsIgnoreCase(source)) {
                    // 设置重定向响应头
                    exchange.getResponseHeaders().set("Location", target);
                    exchange.sendResponseHeaders(statusCode, -1);
                    exchange.close();
                    // 终止链路，不往下走处理器
                    return;
                }
            }

            // 不匹配任何规则，正常放行
            chain.doFilter(exchange);
        }

        @Override
        public String description() {
            return "URL Redirect Filter";
        }
    }

    private static class AgentHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Content-Type", "text/html;charset=UTF-8");
            try (InputStream in = getClass()
                    .getResourceAsStream("/com/lealone/server/http/web/agent.html")) {
                exchange.sendResponseHeaders(200, in.available());
                OutputStream out = exchange.getResponseBody();
                IOUtils.copy(in, out);
                out.flush();
                exchange.close();
            }
        }
    }
}
