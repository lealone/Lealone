/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.http;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.service.ServiceHandler;
import com.lealone.server.http.container.servlets.DefaultServlet;
import com.lealone.server.http.util.descriptor.FilterDef;
import com.lealone.server.http.util.descriptor.FilterMap;
import com.lealone.server.servlet.FilterChain;
import com.lealone.server.servlet.Servlet;
import com.lealone.server.servlet.ServletException;
import com.lealone.server.servlet.http.HttpFilter;
import com.lealone.server.servlet.http.HttpServlet;
import com.lealone.server.servlet.http.HttpServletRequest;
import com.lealone.server.servlet.http.HttpServletResponse;
import com.lealone.server.servlet.http.Part;
import com.lealone.server.template.TemplateEngine;

public class HttpRouter {

    private static final Logger log = LoggerFactory.getLogger(HttpRouter.class);

    protected HttpServer httpServer;
    protected String webRoot;
    protected String uploadDirectory;

    private int filterId;
    private int servletId;

    public void init(HttpServer server, Map<String, String> config) {
        httpServer = server;
        webRoot = config.get("web_root");
        uploadDirectory = config.get("upload_directory");
        if (uploadDirectory == null)
            uploadDirectory = webRoot + "/file_uploads";
        init(config);
    }

    public void init(Map<String, String> config) {
        initFilters(config);
        addServlet("serviceServlet", new HttpServiceServlet(new ServiceHandler(config)), "/service/*");
        addServlet("defaultServlet", new HttpDefaultServlet(config), "/");
        addServlet("agentServlet", new AgentServlet(), "/agent");
        if (isDevelopmentEnvironment(config)) {
            addFilter(new TemplateFilter(), "*.html");
        }
        httpServer.getContext().addWelcomeFile("index.html");
    }

    private void initFilters(Map<String, String> config) {
        for (Entry<String, String> e : config.entrySet()) {
            String key = e.getKey();
            if (key.equalsIgnoreCase("redirect_filter")) {
                for (String redirect : StringUtils.arraySplit(e.getValue(), ',')) {
                    String[] a = StringUtils.arraySplit(redirect, ':');
                    addRedirectFilter(a[0], a[1]);
                }
            } else if (key.equalsIgnoreCase("file_upload_filter")) {
                for (String urlPattern : StringUtils.arraySplit(e.getValue(), ',')) {
                    addFileUploadFilter(urlPattern);
                }
            } else if (key.equalsIgnoreCase("filters")) {
                for (String filterAndUrlPattern : StringUtils.arraySplit(e.getValue(), ';')) {
                    String[] a = StringUtils.arraySplit(filterAndUrlPattern, ':');
                    String filterClassName = a[0];
                    try {
                        for (String urlPattern : StringUtils.arraySplit(a[1], ',')) {
                            HttpFilter filter = Utils.newInstance(filterClassName);
                            addFilter(filter, urlPattern);
                        }
                    } catch (Throwable t) {
                        log.warn("", t);
                    }
                }
            }
        }
    }

    public void addServlet(Servlet servlet, String urlPattern) {
        addServlet(servlet.getClass().getName() + (servletId++), servlet, urlPattern);
    }

    public void addServlet(String name, Servlet servlet, String urlPattern) {
        httpServer.addServlet(name, servlet);
        httpServer.addServletMappingDecoded(urlPattern, name);
    }

    public void addFilter(HttpFilter filter, String urlPattern) {
        addFilter(filter.getClass().getName() + (filterId++), filter, urlPattern);
    }

    public void addFilter(String name, HttpFilter filter, String urlPattern) {
        FilterDef def = new FilterDef();
        def.setFilter(filter);
        def.setFilterName(name);
        def.setFilterClass(filter.getClass().getName());
        FilterMap map = new FilterMap();
        map.setFilterName(name);
        map.addURLPattern(urlPattern);
        httpServer.getContext().addFilterDef(def);
        httpServer.getContext().addFilterMap(map);
    }

    private boolean isDevelopmentEnvironment(Map<String, String> config) {
        String environment = config.get("environment");
        if (environment != null) {
            environment = environment.trim().toLowerCase();
            if (environment.equals("development") || environment.equals("dev")) {
                return true;
            }
        }
        return false;
    }

    private class TemplateFilter extends HttpFilter {
        @Override
        protected void doFilter(HttpServletRequest request, HttpServletResponse response,
                FilterChain chain) throws IOException, ServletException {
            TemplateEngine te = new TemplateEngine(webRoot, "utf-8");
            String file = request.getRequestURI();
            try {
                String str = te.process(file);
                response.setCharacterEncoding("UTF-8");
                response.setContentType("text/html; charset=utf-8");
                response.getWriter().write(str);
            } catch (Exception e) {
            }
        }
    }

    public void addFileUploadFilter(String urlPattern) {
        addFilter(new FileUploadFilter(), urlPattern);
        httpServer.getContext().setAllowCasualMultipartParsing(true);
    }

    private class FileUploadFilter extends HttpFilter {
        @Override
        protected void doFilter(HttpServletRequest request, HttpServletResponse response,
                FilterChain chain) throws IOException, ServletException {
            for (Part p : request.getParts()) {
                String submittedFileName = p.getSubmittedFileName();
                if (submittedFileName != null && !submittedFileName.isBlank()) {
                    File uploadFile = new File(uploadDirectory, submittedFileName);
                    if (!uploadFile.getParentFile().exists())
                        uploadFile.getParentFile().mkdirs();
                    p.write(uploadFile.getCanonicalPath());
                }
            }
            chain.doFilter(request, response);
        }
    }

    public void addRedirectFilter(String urlPattern, String location) {
        addFilter(new RedirectFilter(location), urlPattern);
    }

    private static class RedirectFilter extends HttpFilter {

        private final String location;

        public RedirectFilter(String location) {
            this.location = location;
        }

        @Override
        protected void doFilter(HttpServletRequest request, HttpServletResponse response,
                FilterChain chain) throws IOException, ServletException {
            if (location.charAt(0) == '@') {
                response.sendRedirect(request.getParameter(location.substring(1)));
            } else {
                response.sendRedirect(location);
            }
        }
    }

    private static class HttpDefaultServlet extends DefaultServlet {

        private final String characterEncoding;

        private HttpDefaultServlet(Map<String, String> config) {
            String ce = config.get("character_encoding");
            if (ce == null)
                characterEncoding = "UTF-8";
            else
                characterEncoding = ce;
        }

        @Override
        protected void service(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.setCharacterEncoding(characterEncoding);
            super.service(request, response);
        }
    }

    private static class AgentServlet extends HttpServlet {
        @Override
        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            try (InputStream in = getClass()
                    .getResourceAsStream("/com/lealone/server/http/web/agent.html");) {
                response.setContentType("text/html;charset=UTF-8");
                OutputStream out = response.getOutputStream();
                IOUtils.copy(in, out);
                out.flush();
            }
        }
    }
}
