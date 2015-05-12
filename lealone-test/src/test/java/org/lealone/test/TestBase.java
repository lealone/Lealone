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
    private static String storageEngineName = Constants.DEFAULT_STORAGE_ENGINE_NAME;
    private static boolean embedded = true;
    private static boolean in_memory = false;

    private static String host = "localhost";
    private static int port = Constants.DEFAULT_TCP_PORT;

    public static void addConnectionParameter(String key, String value) {
        connectionParameters.put(key, value);
    }

    public static void setStorageEngineName(String name) {
        storageEngineName = name;
    }

    public static void setEmbedded(boolean embedded) {
        TestBase.embedded = embedded;
    }

    public static void setInMemory(boolean in_memory) {
        TestBase.in_memory = in_memory;
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

    public static void printURL() {
        System.out.println("JDBC URL: " + getURL());
        System.out.println();
    }

    public static String getURL() {
        //addConnectionParameter("DATABASE_TO_UPPER", "false");
        //addConnectionParameter("ALIAS_COLUMN_NAME", "true");

        StringBuilder url = new StringBuilder(100);

        url.append(Constants.URL_PREFIX);
        if (in_memory)
            url.append(Constants.URL_MEM);

        if (embedded) {
            url.append(Constants.URL_EMBED);
            if (!in_memory)
                url.append(TEST_DIR).append('/');
        } else {
            url.append(Constants.URL_TCP).append("//").append(host).append(':').append(port).append('/');
        }

        url.append(DB_NAME).append("?default_storage_engine=").append(storageEngineName);

        for (Map.Entry<String, String> e : connectionParameters.entrySet())
            url.append(';').append(e.getKey()).append('=').append(e.getValue());

        return url.toString();
    }

    public static Connection getConnection() throws Exception {
        return DriverManager.getConnection(getURL(), "sa", "");
    }

}
