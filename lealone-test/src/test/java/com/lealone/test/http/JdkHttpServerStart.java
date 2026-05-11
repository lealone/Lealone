/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.http;

import java.util.Map;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.http.HttpServerEngine;
import com.lealone.http.jdk.JdkHttpRouter;
import com.lealone.main.Lealone;
import com.lealone.sql.config.Config;
import com.lealone.sql.config.Config.PluggableEngineDef;
import com.lealone.sql.config.ConfigListener;

public class JdkHttpServerStart extends JdkHttpRouter implements ConfigListener {

    // http://localhost:8080/index.html
    public static void main(String[] args) {
        System.setProperty("lealone.config.listener", JdkHttpServerStart.class.getName());
        Lealone.main(args);
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
        jdkHttpServer.createContext("/test", exchange -> {
            byte[] respContents = "Hello World".getBytes("UTF-8");
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
            exchange.sendResponseHeaders(200, respContents.length);
            exchange.getResponseBody().write(respContents);
            exchange.close();
        });
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (HttpServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = true;
            }
        }
    }
}
