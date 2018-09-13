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

import org.lealone.common.exceptions.ConfigException;
import org.lealone.main.Lealone;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.YamlConfigLoader;
import org.lealone.test.TestBase;

public class NodeBase extends YamlConfigLoader {

    public static void run(Class<?> loader, String[] args) {
        init(loader);
        Lealone.main(args);
    }

    private static void init(Class<?> loader) {
        System.setProperty("lealone.config.loader", loader.getName());
        System.setProperty("lealone.config", "lealone-test.yaml");

        // System.setProperty("lealone.load.persisted.node.info", "false"); // 不从nodes表加载ring状态信息

        // 见P2pServer.getRingDelay()
        // 默认30秒，等太久了
        System.setProperty("lealone.ring.delay.ms", "5000");

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        // System.setProperty("lealone.check2", "true");

        TestBase.optimizeNetty();
        TestBase.optimizeVertx();
    }

    protected String listen_address;
    protected String dir;
    protected String nodeBaseDirPrefix;
    protected String endpoint_snitch;

    // 在org.lealone.common.util.Utils.construct(String, String)中必须使用无参数的构造函数
    public NodeBase() {
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        config.base_dir = config.base_dir + "/" + nodeBaseDirPrefix;
        if (dir != null) {
            config.base_dir = config.base_dir + dir;
        }

        if (listen_address != null)
            config.listen_address = listen_address;

        if (endpoint_snitch != null)
            config.cluster_config.endpoint_snitch = endpoint_snitch;

        System.setProperty("java.io.tmpdir", "./" + config.base_dir + "/tmp");
        System.setProperty("lealone.base.dir", "./" + config.base_dir);

        // System.setProperty(FileResolver.DISABLE_CP_RESOLVING_PROP_NAME, "true");
        System.setProperty("vertx.cacheDirBase", "./" + config.base_dir + "/.vertx");

        super.applyConfig(config);
    }
}
