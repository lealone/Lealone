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
package org.lealone.cluster.config;

import java.util.ArrayList;
import java.util.List;

import org.lealone.cluster.exceptions.ConfigurationException;

public abstract class TransportServerOptions {

    public String listen_address;
    public Integer port;
    public boolean trace = false;
    public boolean allow_others = false;
    public boolean daemon = false;
    public boolean if_exists = false;

    public abstract List<String> getOptions(Config config);

    protected List<String> getCommonOptions(Config config, String prefix) {
        List<String> list = new ArrayList<>();
        list.add("-baseDir");
        list.add(config.base_dir);

        if (listen_address == null)
            listen_address = config.listen_address; //如果没有指定，就用全局的listen_address

        list.add(prefix + "ListenAddress");
        list.add(listen_address);

        //如果没有指定，在初始化TcpServer和PgServer时会自动使用相应的默认值
        if (port != null) {
            list.add(prefix + "Port");
            list.add(port.toString());
        }
        if (trace)
            list.add("-trace");
        if (allow_others)
            list.add(prefix + "AllowOthers");
        if (daemon)
            list.add(prefix + "Daemon");
        if (if_exists)
            list.add("-ifExists");

        return list;
    }

    public static class TcpServerOptions extends TransportServerOptions {
        public boolean ssl = false;
        public String password;
        public String key;

        @Override
        public List<String> getOptions(Config config) {
            List<String> list = getCommonOptions(config, "-tcp");
            if (ssl)
                list.add("-tcpSSL");
            if (password != null) {
                list.add("-tcpPassword");
                list.add(password);
            }
            if (key != null) {
                list.add("-key");
                String[] keyAndDatabase = key.split(",");
                if (keyAndDatabase.length != 2)
                    throw new ConfigurationException("Invalid key option, usage: 'key: v1,v2'.");
                list.add(keyAndDatabase[0]);
                list.add(keyAndDatabase[1]);
            }
            return list;
        }
    }

    public static class PgServerOptions extends TransportServerOptions {
        public boolean enabled = false;

        @Override
        public List<String> getOptions(Config config) {
            return getCommonOptions(config, "-pg");
        }
    }
}
