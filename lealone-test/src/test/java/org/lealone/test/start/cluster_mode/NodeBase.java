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
package org.lealone.test.start.cluster_mode;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.Config.RunMode;
import org.lealone.cluster.config.YamlConfigurationLoader;
import org.lealone.cluster.exceptions.ConfigurationException;

public class NodeBase extends YamlConfigurationLoader {
    public static void init(Class<?> loader, String yaml) {
        System.setProperty("lealone.config.loader", loader.getName());
        System.setProperty("lealone.config", yaml);
        System.setProperty("lealone-foreground", "true"); // 打印输出到控制台

        //System.setProperty("lealone.load.ring.state", "false"); // 不从system.peers表加载ring状态信息

        //见StorageService.getRingDelay()
        //默认30秒，等太久了
        System.setProperty("lealone.ring.delay.ms", "5000");
    }

    protected String listen_address;
    protected String dir;
    protected String nodeBaseDirPrefix;

    protected RunMode run_mode = RunMode.cluster;

    //在org.lealone.cluster.utils.FBUtilities.construct(String, String)中必须使用无参数的构造函数
    public NodeBase() {
    }

    @Override
    public Config loadConfig() throws ConfigurationException {
        Config config = super.loadConfig();

        config.base_dir = config.base_dir + "/" + nodeBaseDirPrefix + dir;
        config.run_mode = run_mode;

        config.listen_address = listen_address;

        System.setProperty("java.io.tmpdir", "./" + config.base_dir + "/tmp");
        System.setProperty("lealone.base.dir", "./" + config.base_dir);

        return config;
    }
}
