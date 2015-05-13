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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.lealone.engine.Constants;

public class TestBase extends Assert {
    public static final String DEFAULT_STORAGE_ENGINE_NAME = Constants.DEFAULT_STORAGE_ENGINE_NAME;
    public static final String TEST_DIR = "./lealone-test-data";
    public static final String DB_NAME = "test";

    private static final Map<String, String> connectionParameters = new HashMap<>();
    private static String storageEngineName;
    private static boolean embedded;
    private static boolean inMemory;
    private static boolean mysqlUrlStyle;

    private static String host;
    private static int port;

    static {
        reset();
    }

    public static synchronized void reset() {
        connectionParameters.clear();
        storageEngineName = Constants.DEFAULT_STORAGE_ENGINE_NAME;
        embedded = false;
        inMemory = false;
        mysqlUrlStyle = false;

        host = Constants.DEFAULT_HOST;
        port = Constants.DEFAULT_TCP_PORT;
    }

    public static synchronized void addConnectionParameter(String key, String value) {
        connectionParameters.put(key, value);
    }

    public static void setStorageEngineName(String name) {
        storageEngineName = name;
    }

    public static void setEmbedded(boolean embedded) {
        TestBase.embedded = embedded;
    }

    public static void setInMemory(boolean inMemory) {
        TestBase.inMemory = inMemory;
    }

    public static void setMysqlUrlStyle(boolean mysqlUrlStyle) {
        TestBase.mysqlUrlStyle = mysqlUrlStyle;
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        TestBase.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        TestBase.port = port;
    }

    public static String getHostAndPort() {
        return host + ":" + port;
    }

    public static void printURL() {
        System.out.println("JDBC URL: " + getURL());
        System.out.println();
    }

    public static synchronized String getURL() {
        //addConnectionParameter("DATABASE_TO_UPPER", "false");
        //addConnectionParameter("ALIAS_COLUMN_NAME", "true");

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

        url.append(DB_NAME).append(firstSeparatorChar).append("default_storage_engine=").append(storageEngineName);

        for (Map.Entry<String, String> e : connectionParameters.entrySet())
            url.append(separatorChar).append(e.getKey()).append('=').append(e.getValue());

        return url.toString();
    }

    public static Connection getConnection() throws Exception {
        return DriverManager.getConnection(getURL());
    }
}
