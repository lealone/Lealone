/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.main;

import java.util.ArrayList;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.StringUtils;
import com.lealone.http.tomcat.TomcatServerEngine;
import com.lealone.service.http.HttpRouter;
import com.lealone.sql.config.Config;

public class LealoneApplication {

    private final CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>();
    private final ArrayList<String> argList = new ArrayList<>();
    private String protocolServerEngine = TomcatServerEngine.NAME;

    public void setProtocolServerEngine(String protocolServerEngine) {
        this.protocolServerEngine = protocolServerEngine;
    }

    public void setBaseDir(String baseDir) {
        argList.add("-baseDir");
        argList.add(baseDir);
    }

    public void setDatabase(String dbName) {
        argList.add("-database");
        argList.add(dbName);
        config.put("default_database", dbName);
    }

    public void setSchema(String schemaName) {
        config.put("default_schema", schemaName);
    }

    public void setInitSql(String initSql) {
        argList.add("-initSql");
        argList.add(initSql);
    }

    public void setSqlScripts(String... sqlScripts) {
        argList.add("-sqlScripts");
        argList.add(StringUtils.arrayCombine(sqlScripts, ','));
    }

    public void setWebRoot(String webRoot) {
        config.put("web_root", webRoot);
    }

    public void setJdbcUrl(String jdbcUrl) {
        config.put("jdbc_url", jdbcUrl);
        System.setProperty(com.lealone.db.Constants.JDBC_URL_KEY, jdbcUrl);
    }

    public void setHost(String host) {
        config.put("host", host);
    }

    public void setPort(int port) {
        config.put("port", String.valueOf(port));
    }

    public void setEnvironment(String env) {
        config.put("environment", env);
    }

    public void setRouter(String router) {
        config.put("router", router);
    }

    public void setRouter(Class<? extends HttpRouter> router) {
        setRouter(router.getCanonicalName());
    }

    public void start() {
        String[] args = new String[argList.size()];
        argList.toArray(args);
        config.put("name", protocolServerEngine);
        config.put("enabled", "true");
        if (!config.containsKey("default_schema"))
            config.put("default_schema", "public");
        Config lealoneConfig = Lealone.createConfig();
        Config.mergeEngines(config, lealoneConfig.protocol_server_engines);
        new Lealone().start(args, lealoneConfig);
    }

    public static void start(String dbName, String... sqlScripts) {
        LealoneApplication app = new LealoneApplication();
        app.setDatabase(dbName);
        app.setSqlScripts(sqlScripts);
        app.start();
    }

    public static void start(String[] args) {
        Lealone.main(args);
    }
}
