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
package org.lealone.server;

import java.util.Map;

public abstract class ProtocolServerEngineBase implements ProtocolServerEngine {

    protected final String name;

    // 目前用不到
    // protected Map<String, String> config;

    public ProtocolServerEngineBase(String name) {
        this.name = name;
        // 见PluggableEngineManager.PluggableEngineService中的注释
        ProtocolServerEngineManager.getInstance().registerEngine(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init(Map<String, String> config) {
        // this.config = config;
    }

    @Override
    public void close() {
    }

    @Override
    public void stopProtocolServer(int port) {
        getProtocolServer(port).stop();
    }

    protected abstract ProtocolServer getProtocolServer(int port);

}
