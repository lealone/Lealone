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

import org.lealone.aose.config.Config;
import org.lealone.aose.config.YamlConfigurationLoader;
import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.main.Lealone;

import io.vertx.core.impl.FileResolver;

public class NodeBase extends YamlConfigurationLoader {

    public static void run(Class<?> loader, String[] args) {
        init(loader);
        Lealone.main(args);
    }

    private static void init(Class<?> loader) {
        System.setProperty("lealone.config.loader", loader.getName());
        System.setProperty("lealone.config", "lealone-test.yaml");
        System.setProperty("lealone-foreground", "true"); // 打印输出到控制台

        // System.setProperty("lealone.load.ring.state", "false"); // 不从system.peers表加载ring状态信息

        // 见StorageService.getRingDelay()
        // 默认30秒，等太久了
        System.setProperty("lealone.ring.delay.ms", "5000");

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        // System.setProperty("lealone.check2", "true");
    }

    protected Integer host_id;
    protected String listen_address;
    protected String dir;
    protected String nodeBaseDirPrefix;
    protected String endpoint_snitch;

    // 在org.lealone.common.util.Utils.construct(String, String)中必须使用无参数的构造函数
    public NodeBase() {
    }

    @Override
    public void applyConfig(Config config) throws ConfigurationException {
        if (host_id != null)
            config.host_id = host_id;

        config.base_dir = config.base_dir + "/" + nodeBaseDirPrefix;
        if (dir != null) {
            config.base_dir = config.base_dir + dir;
        }

        config.listen_address = listen_address;

        if (endpoint_snitch != null)
            config.endpoint_snitch = endpoint_snitch;

        System.setProperty("java.io.tmpdir", "./" + config.base_dir + "/tmp");
        System.setProperty("lealone.base.dir", "./" + config.base_dir);

        System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty("vertx.cacheDirBase", "./" + config.base_dir + "/.vertx");

        super.applyConfig(config);
    }
}
