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
package org.lealone.cbase.cluster.standalone;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.YamlConfigurationLoader;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.service.LealoneDaemon;

public class LealoneStandalone extends YamlConfigurationLoader {

    public static void main(String[] args) {
        System.setProperty("lealone.join_ring", "false"); //不加入ring，因为是单独运行
        System.setProperty("lealone.load_ring_state", "true"); //从system.peers表加载ring状态信息
        setConfigLoader(LealoneStandalone.class);
        run(args, "lealone-onedc.yaml");
    }

    protected String listen_address;
    protected String dir;

    //在org.lealone.cluster.utils.FBUtilities.construct(String, String)中必须使用无参数的构造函数
    public LealoneStandalone() {
        this.listen_address = "127.0.0.1";
        this.dir = "standalone";
    }

    @Override
    public Config loadConfig() throws ConfigurationException {

        this.dir = "lealone-test-data/" + dir + "/";
        Config config = super.loadConfig();
        config.listen_address = listen_address;
        config.commitlog_directory = dir + "commitlog";
        config.saved_caches_directory = dir + "saved_caches";
        config.data_file_directories = new String[] { dir + "data" };

        System.setProperty("java.io.tmpdir", "./" + config.data_file_directories[0] + "/tmp");
        System.setProperty("lealone.base.dir", "./" + config.data_file_directories[0] + "/cbase");

        //config.dynamic_snitch_update_interval_in_ms = 100000;
        return config;
    }

    public static void setConfigLoader(Class<?> clz) {
        System.setProperty("lealone.config.loader", clz.getName());
    }

    public static void run(String[] args, String yaml) {

        System.setProperty("lealone.rpc_port", "9160");
        System.setProperty("lealone.start_native_transport", "true"); // 启用native
                                                                      // server，用于支持CQL
        System.setProperty("lealone.native_transport_port", "9042");

        System.setProperty("lealone.config", yaml);

        System.setProperty("lealone.start_rpc", "false"); // 不启用thrift server

        System.setProperty("lealone-foreground", "true"); // 打印输出到控制台

        System.setProperty("lealone-pidfile", "pidfile.txt");

        //System.setProperty("lealone.load_ring_state", "false"); // 不从system.peers表加载ring状态信息

        //见org.apache.lealone.service.StorageService.getRingDelay()
        //默认30秒，等太久了
        System.setProperty("lealone.ring_delay_ms", "5000");

        System.setProperty("lealone.unsafesystem", "true"); //不要每次更新元数据就刷新到硬盘，产生大量文件，只在测试时用

        LealoneDaemon.main(args);
    }

}
