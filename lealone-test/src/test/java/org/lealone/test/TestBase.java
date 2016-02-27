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
import org.lealone.db.Constants;
import org.lealone.db.SysProperties;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;

public class TestBase extends Assert {
    public static final String DEFAULT_STORAGE_ENGINE_NAME = Constants.DEFAULT_STORAGE_ENGINE_NAME;
    public static final String TEST_DIR = "." + File.separatorChar + "lealone-test-data" + File.separatorChar + "test";
    public static final String DB_NAME = "test";

    public static TransactionEngine te;

    static {
        SysProperties.setBaseDir(TEST_DIR);
    }

    public static synchronized void initTransactionEngine() {
        if (te == null) {
            te = TransactionEngineManager.getInstance().getEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);

            Map<String, String> config = new HashMap<>();
            config.put("base_dir", TEST_DIR);
            config.put("transaction_log_dir", "tlog");
            // config.put("log_sync_type", LogStorage.LOG_SYNC_TYPE_NO_SYNC);
            te.init(config);
        }
    }

    private final Map<String, String> connectionParameters = new HashMap<>();
    private String storageEngineName = Constants.DEFAULT_STORAGE_ENGINE_NAME;
    private boolean embedded = false;
    private boolean inMemory = false;
    private boolean mysqlUrlStyle = false;

    private String host = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    public static String joinDirs(String... dirs) {
        StringBuilder s = new StringBuilder(TEST_DIR);
        for (String dir : dirs)
            s.append(File.separatorChar).append(dir);
        return s.toString();
    }

    public synchronized void addConnectionParameter(String key, String value) {
        connectionParameters.put(key, value);
    }

    public void setStorageEngineName(String name) {
        storageEngineName = name;
    }

    public void setEmbedded(boolean embedded) {
        this.embedded = embedded;
    }

    public void setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
    }

    public void setMysqlUrlStyle(boolean mysqlUrlStyle) {
        this.mysqlUrlStyle = mysqlUrlStyle;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostAndPort() {
        return host + ":" + port;
    }

    public void printURL() {
        System.out.println("JDBC URL: " + getURL());
        System.out.println();
    }

    public synchronized String getURL() {
        return getURL(DB_NAME);
    }

    public synchronized String getURL(String user, String password) {
        connectionParameters.put("user", user);
        connectionParameters.put("password", password);
        return getURL();
    }

    public synchronized String getURL(String dbName) {
        // addConnectionParameter("DATABASE_TO_UPPER", "false");
        // addConnectionParameter("ALIAS_COLUMN_NAME", "true");
        // addConnectionParameter("IGNORE_UNKNOWN_SETTINGS", "true");

        if (!connectionParameters.containsKey("user")) {
            addConnectionParameter("user", "sa");
            addConnectionParameter("password", "");
        }

        StringBuilder url = new StringBuilder(100);

        url.append(Constants.URL_PREFIX);
        if (inMemory)
            url.append(Constants.URL_MEM);

        if (embedded) {
            url.append(Constants.URL_EMBED);
            if (!inMemory)
                url.append(TEST_DIR).append('/');
        } else {
            url.append(Constants.URL_TCP).append("//").append(host).append(':').append(port).append('/');
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

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }
}
