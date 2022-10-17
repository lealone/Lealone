/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.tomcat;

import java.util.Map;

import org.apache.catalina.servlets.DefaultServlet;
import org.apache.tomcat.util.descriptor.web.FilterDef;
import org.apache.tomcat.util.descriptor.web.FilterMap;
import org.lealone.server.http.HttpRouter;
import org.lealone.server.http.HttpServer;
import org.lealone.server.service.ServiceHandler;

public class TomcatRouter implements HttpRouter {
    @Override
    public void init(HttpServer server, Map<String, String> config) {
        TomcatServer tomcatServer = (TomcatServer) server;

        tomcatServer.addServlet("serviceServlet", new TomcatServiceServlet(new ServiceHandler(config)));
        tomcatServer.addServlet("defaultServlet", new DefaultServlet());

        tomcatServer.addServletMappingDecoded("/service/*", "serviceServlet");
        tomcatServer.addServletMappingDecoded("/", "defaultServlet");

        FilterDef def = new FilterDef();
        def.setFilter(new TomcatServiceFilter(new ServiceHandler(config)));
        def.setFilterName("serviceFilter");
        FilterMap map = new FilterMap();
        map.setFilterName("serviceFilter");
        map.addServletName("serviceServlet");
        tomcatServer.getContext().addFilterDef(def);
        tomcatServer.getContext().addFilterMap(map);
        tomcatServer.getContext().addWelcomeFile("index.html");
    }
}
