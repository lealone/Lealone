/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.http.tomcat;

import java.io.IOException;
import java.util.Map;

import org.apache.catalina.servlets.DefaultServlet;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;

import com.lealone.service.ServiceHandler;
import com.lealone.service.http.HttpRouter;
import com.lealone.service.http.HttpServer;

import jakarta.servlet.Servlet;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class TomcatRouter implements HttpRouter {

    protected TomcatServer tomcatServer;

    @Override
    public void init(HttpServer server, Map<String, String> config) {
        tomcatServer = (TomcatServer) server;
        init(config);
    }

    public void init(Map<String, String> config) {
        addServlet("serviceServlet", new TomcatServiceServlet(new ServiceHandler(config)), "/service/*");
        addServlet("defaultServlet", new TomcatDefaultServlet(config), "/");
        tomcatServer.getContext().addWelcomeFile("index.html");
    }

    public void addServlet(Servlet servlet, String urlPattern) {
        addServlet(servlet.getClass().getName(), servlet, urlPattern);
    }

    public void addServlet(String name, Servlet servlet, String urlPattern) {
        tomcatServer.addServlet(name, servlet);
        tomcatServer.addServletMappingDecoded(urlPattern, name);
    }

    public void addFilter(HttpFilter filter, String urlPattern) {
        addFilter(filter.getClass().getName(), filter, urlPattern);
    }

    public void addFilter(String name, HttpFilter filter, String urlPattern) {
        FilterDef def = new FilterDef();
        def.setFilter(filter);
        def.setFilterName(name);
        FilterMap map = new FilterMap();
        map.setFilterName(name);
        map.addURLPattern(urlPattern);
        tomcatServer.getContext().addFilterDef(def);
        tomcatServer.getContext().addFilterMap(map);
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
}
