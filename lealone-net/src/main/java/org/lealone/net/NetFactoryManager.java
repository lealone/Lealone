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
package org.lealone.net;

import java.util.Map;

import org.lealone.db.Constants;
import org.lealone.db.PluggableEngineManager;

public class NetFactoryManager extends PluggableEngineManager<NetFactory> {

    private static final NetFactoryManager instance = new NetFactoryManager();

    public static NetFactoryManager getInstance() {
        return instance;
    }

    private NetFactoryManager() {
        super(NetFactory.class);
    }

    public static NetFactory getFactory(String name) {
        return instance.getEngine(name);
    }

    public static NetFactory getFactory(Map<String, String> config) {
        String netFactoryName = config.get(Constants.NET_FACTORY_NAME_KEY);
        if (netFactoryName == null)
            netFactoryName = Constants.DEFAULT_NET_FACTORY_NAME;

        NetFactory factory = getFactory(netFactoryName);
        if (factory == null) {
            throw new RuntimeException("NetFactory '" + netFactoryName + "' can not found");
        }
        return factory;
    }

    public static void registerFactory(NetFactory factory) {
        instance.registerEngine(factory);
    }

    public static void deregisterFactory(NetFactory factory) {
        instance.deregisterEngine(factory);
    }
}
