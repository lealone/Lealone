/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.http;

import java.util.Map;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.http.tomcat.TomcatRouter;
import com.lealone.http.tomcat.TomcatServerEngine;
import com.lealone.main.Lealone;
import com.lealone.sql.config.Config;
import com.lealone.sql.config.Config.PluggableEngineDef;
import com.lealone.sql.config.ConfigListener;
import com.lealone.test.http.EmbedTomcatStart.TestServlet;

public class LealoneTomcatStart extends TomcatRouter implements ConfigListener {

    // http://localhost:8080/index.html
    public static void main(String[] args) {
        System.setProperty("lealone.config.listener", LealoneTomcatStart.class.getName());
        Lealone.main(args);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        tomcatServer.addServlet("testServlet", new TestServlet());
        tomcatServer.addServletMappingDecoded("/test", "testServlet");
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (TomcatServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = true;
            }
        }
    }
}
