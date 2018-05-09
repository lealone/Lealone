/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.lealone.aose.config.Config;
import org.lealone.common.trace.TraceSystem;
import org.lealone.db.Constants;
import org.lealone.db.SysProperties;
import org.lealone.mvcc.log.RedoLog;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;

import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.JdkLoggerFactory;
import io.vertx.core.impl.FileResolver;
import io.vertx.core.spi.resolver.ResolverProvider;

public class TestBase extends Assert {
    public static String url;
    public static final String DEFAULT_STORAGE_ENGINE_NAME = getDefaultStorageEngineName();
    public static final String TEST_BASE_DIR = "." + File.separatorChar + "target" + File.separatorChar + "test-data";
    public static final String TEST_DIR = TEST_BASE_DIR + File.separatorChar + "test";
    public static final String TEST = "test";
    public static final String LEALONE = "lealone";
    public static final String DEFAULT_DB_NAME = TEST;
    public static final String DEFAULT_USER = "root";
    public static final String DEFAULT_PASSWORD = "";

    public static TransactionEngine te;

    static {
        System.setProperty("java.io.tmpdir", TEST_DIR + File.separatorChar + "tmp");
        System.setProperty("lealone.lob.client.max.size.memory", "2048");

        Config.setProperty("client.trace.directory", joinDirs("client_trace"));
        SysProperties.setBaseDir(TEST_DIR);

        if (Config.getProperty("default.storage.engine") == null)
            Config.setProperty("default.storage.engine", getDefaultStorageEngineName());

        setVertxProperties();
    }

    private static void setVertxProperties() {
        InternalLoggerFactory.setDefaultFactory(JdkLoggerFactory.INSTANCE);
        System.setProperty(ResolverProvider.DISABLE_DNS_RESOLVER_PROP_NAME, "true");

        System.setProperty(FileResolver.DISABLE_FILE_CACHING_PROP_NAME, "true");
        System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty(FileResolver.CACHE_DIR_BASE_PROP_NAME, "./" + TEST_DIR + "/.vertx");
    }

    public static String getDefaultStorageEngineName() {
        return "AOSE";
    }

    public static synchronized void initTransactionEngine() {
        if (te == null) {
            te = TransactionEngineManager.getInstance().getEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);

            Map<String, String> config = new HashMap<>();
            config.put("base_dir", TEST_DIR);
            config.put("transaction_log_dir", "tlog");
            config.put("log_sync_type", RedoLog.LOG_SYNC_TYPE_PERIODIC);
            te.init(config);
        }
    }

    protected String dbName = DEFAULT_DB_NAME;
    protected String user = DEFAULT_USER;
    protected String password = DEFAULT_PASSWORD;

    private final Map<String, String> connectionParameters = new HashMap<>();
    private String storageEngineName = getDefaultStorageEngineName();
    private boolean embedded = false;
    private boolean inMemory = false;
    private boolean mysqlUrlStyle = false;
    private boolean ssl = false;

    private String host = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    public static String joinDirs(String... dirs) {
        StringBuilder s = new StringBuilder(TEST_DIR);
        for (String dir : dirs)
            s.append(File.separatorChar).append(dir);
        return s.toString();
    }

    public synchronized TestBase addConnectionParameter(String key, String value) {
        connectionParameters.put(key, value);
        return this;
    }

    public synchronized TestBase addConnectionParameter(String key, Object value) {
        connectionParameters.put(key, value.toString());
        return this;
    }

    public TestBase enableTrace() {
        return enableTrace(TraceSystem.INFO);
    }

    public TestBase enableTrace(int level) {
        addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", level + "");
        return this;
    }

    public TestBase enableSSL() {
        ssl = true;
        return this;
    }

    public TestBase setStorageEngineName(String name) {
        storageEngineName = name;
        return this;
    }

    public TestBase setEmbedded(boolean embedded) {
        this.embedded = embedded;
        return this;
    }

    public TestBase setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
        return this;
    }

    public TestBase setMysqlUrlStyle(boolean mysqlUrlStyle) {
        this.mysqlUrlStyle = mysqlUrlStyle;
        return this;
    }

    public String getHost() {
        return host;
    }

    public TestBase setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public TestBase setPort(int port) {
        this.port = port;
        return this;
    }

    public String getHostAndPort() {
        return host + ":" + port;
    }

    public void printURL() {
        System.out.println("JDBC URL: " + getURL());
        System.out.println();
    }

    public synchronized String getURL() {
        return getURL(dbName);
    }

    public synchronized String getURL(String user, String password) {
        connectionParameters.put("user", user);
        connectionParameters.put("password", password);
        return getURL();
    }

    public synchronized String getURL(String dbName) {
        if (url != null)
            return url;
        // addConnectionParameter("DATABASE_TO_UPPER", "false");
        // addConnectionParameter("ALIAS_COLUMN_NAME", "true");
        // addConnectionParameter("IGNORE_UNKNOWN_SETTINGS", "true");

        if (!connectionParameters.containsKey("user")) {
            addConnectionParameter("user", DEFAULT_USER);
            addConnectionParameter("password", DEFAULT_PASSWORD);
        }

        StringBuilder url = new StringBuilder(100);

        url.append(Constants.URL_PREFIX);
        if (inMemory) {
            addConnectionParameter("PERSISTENT", "false");
        }

        if (embedded) {
            url.append(Constants.URL_EMBED);
            if (!inMemory)
                url.append(TEST_DIR).append('/');
        } else {
            if (ssl)
                url.append(Constants.URL_SSL);
            else
                url.append(Constants.URL_TCP);
            url.append("//").append(host).append(':').append(port).append('/');
        }

        char firstSeparatorChar = ';';
        char separatorChar = ';';
        if (mysqlUrlStyle) {
            firstSeparatorChar = '?';
            separatorChar = '&';
        }

        url.append(dbName).append(firstSeparatorChar).append("default_storage_engine=").append(storageEngineName);

        for (Map.Entry<String, String> e : connectionParameters.entrySet())
            url.append(separatorChar).append(e.getKey()).append('=').append(e.getValue());

        return url.toString();
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(getURL());
    }

    public Connection getConnection(String dbName) throws Exception {
        return DriverManager.getConnection(getURL(dbName));
    }

    public Connection getConnection(String user, String password) throws Exception {
        return DriverManager.getConnection(getURL(user, password));
    }

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }
}
