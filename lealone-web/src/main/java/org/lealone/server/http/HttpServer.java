/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side  License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.http;

import java.util.Map;

import org.lealone.server.tomcat.TomcatServer;
import org.lealone.server.vertx.VertxServer;
import org.lealone.server.vertx.VertxServerEngine;

public interface HttpServer {

    String getWebRoot();

    void setWebRoot(String webRoot);

    String getJdbcUrl();

    void setJdbcUrl(String jdbcUrl);

    String getHost();

    void setHost(String host);

    int getPort();

    void setPort(int port);

    void init(Map<String, String> config);

    void start();

    void stop();

    public static HttpServer create() {
        return create(null);
    }

    public static HttpServer create(String type) {
        if (type == null || type.equalsIgnoreCase(VertxServerEngine.NAME))
            return new VertxServer();
        else
            return new TomcatServer();
    }
}
