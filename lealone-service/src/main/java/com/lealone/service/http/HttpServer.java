/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side  License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.service.http;

import java.util.Map;

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

}
