/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.catalina.servlets.DefaultServlet;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;

import com.lealone.common.util.IOUtils;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.http.HttpRouter;
import com.lealone.http.HttpServer;
import com.lealone.http.template.TemplateEngine;
import com.lealone.service.ServiceHandler;

import jakarta.servlet.FilterChain;
import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;

public class TomcatRouter implements HttpRouter {

    private static final Log log = LogFactory.getLog(TomcatRouter.class);

    protected TomcatServer tomcatServer;
    protected String webRoot;
    protected String uploadDirectory;

    private int filterId;
    private int servletId;

    @Override
    public void init(HttpServer server, Map<String, String> config) {
        tomcatServer = (TomcatServer) server;
        webRoot = config.get("web_root");
        uploadDirectory = config.get("upload_directory");
        if (uploadDirectory == null)
            uploadDirectory = webRoot + "/file_uploads";
        init(config);
    }

    public void init(Map<String, String> config) {
        initFilters(config);
        addServlet("serviceServlet", new TomcatServiceServlet(new ServiceHandler(config)), "/service/*");
        addServlet("defaultServlet", new TomcatDefaultServlet(config), "/");
        addServlet("agentServlet", new AgentServlet(), "/agent");
        if (isDevelopmentEnvironment(config)) {
            addFilter(new TemplateFilter(), "*.html");
        }
        tomcatServer.getContext().addWelcomeFile("index.html");
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
        tomcatServer.addServlet(name, servlet);
        tomcatServer.addServletMappingDecoded(urlPattern, name);
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
        tomcatServer.getContext().addFilterDef(def);
        tomcatServer.getContext().addFilterMap(map);
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
        tomcatServer.getContext().setAllowCasualMultipartParsing(true);
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

    private static class TomcatDefaultServlet extends DefaultServlet {

        private final String characterEncoding;

        private TomcatDefaultServlet(Map<String, String> config) {
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
            try (InputStream in = getClass().getResourceAsStream("/web/agent.html");
                    OutputStream out = response.getOutputStream()) {
                response.setContentType("text/html;charset=UTF-8");
                IOUtils.copy(in, out);
            }
        }
    }
}
