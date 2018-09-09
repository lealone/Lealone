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
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.p2p.server.P2pServerEngine;

//-javaagent:E:\continuations\target\continuations-1.0-SNAPSHOT.jar
public class TcpServerStart extends NodeBase {

    // YamlConfigLoader的子类必须有一个无参数的构造函数
    public TcpServerStart() {
        nodeBaseDirPrefix = "client-server";
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        for (PluggableEngineDef e : config.protocol_server_engines) {
            if (P2pServerEngine.NAME.equalsIgnoreCase(e.name)) {
                e.enabled = false;
            }
        }
        super.applyConfig(config);
    }

    public static void main(String[] args) {
        NodeBase.run(TcpServerStart.class, args);
    }

}
