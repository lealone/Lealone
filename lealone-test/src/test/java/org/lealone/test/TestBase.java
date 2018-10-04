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
import org.lealone.common.trace.TraceSystem;
import org.lealone.db.Constants;
import org.lealone.db.SysProperties;
import org.lealone.p2p.config.Config;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.memory.MemoryStorageEngine;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;
import org.lealone.transaction.mvcc.log.RedoLog;

public class TestBase extends Assert {

    public static interface SqlExecuter {
        void execute(String sql);
    }

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
    }

    public TestBase() {
    }

    public static String getDefaultStorageEngineName() {
        return "AOSE";
    }

    public static synchronized void initTransactionEngine() {
        if (te == null) {
            te = TransactionEngineManager.getInstance().getEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);

            Map<String, String> config = new HashMap<>();
            config.put("base_dir", TEST_DIR);
            config.put("redo_log_dir", "redo_log");
            config.put("log_sync_type", RedoLog.LOG_SYNC_TYPE_PERIODIC);
            te.init(config);
        }
    }

    public static synchronized void closeTransactionEngine() {
        if (te != null) {
            te.close();
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

    private String netFactoryName = Constants.DEFAULT_NET_FACTORY_NAME;

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

    public TestBase setNetFactoryName(String name) {
        netFactoryName = name;
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
        if (inMemory || MemoryStorageEngine.NAME.equalsIgnoreCase(storageEngineName)) {
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
        url.append(firstSeparatorChar).append(Constants.NET_FACTORY_NAME_KEY).append("=").append(netFactoryName);

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

    public static void deleteFileRecursive(String path) {
        // 避免误删除
        if (!path.startsWith(TEST_BASE_DIR)) {
            throw new RuntimeException("invalid path: " + path + ", must be start with: " + TEST_BASE_DIR);
        }
        FileUtils.deleteRecursive(path, false);
    }
}
