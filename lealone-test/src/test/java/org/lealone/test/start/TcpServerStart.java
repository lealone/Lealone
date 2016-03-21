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
package org.lealone.test.start;

import io.vertx.core.impl.FileResolver;

import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.main.Lealone;
import org.lealone.main.config.Config;

//-javaagent:E:\continuations\target\continuations-1.0-SNAPSHOT.jar
public class TcpServerStart extends org.lealone.main.config.YamlConfigurationLoader {

    // YamlConfigurationLoader的子类必须有一个无参数的构造函数
    public TcpServerStart() {
    }

    @Override
    public Config loadConfig() throws ConfigurationException {
        Config config = super.loadConfig();

        System.setProperty("java.io.tmpdir", "./" + config.base_dir + "/tmp");
        // System.setProperty("lealone.base.dir", "./" + config.base_dir);

        System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty("vertx.cacheDirBase", "./" + config.base_dir + "/.vertx");

        return config;
    }

    public static void init(Class<?> loader) {
        System.setProperty("lealone.config.loader", loader.getName());
        System.setProperty("lealone.config", "lealone-test.yaml");
        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        // System.setProperty("lealone.check2", "true");
    }

    public static void main(String[] args) {
        init(TcpServerStart.class);
        Lealone.main(args);
    }

}
