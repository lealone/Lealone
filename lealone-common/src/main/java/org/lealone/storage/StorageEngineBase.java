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
package org.lealone.storage;

import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DataHandler;

public abstract class StorageEngineBase implements StorageEngine {

    protected final String name;

    protected Map<String, String> config;

    public StorageEngineBase(String name) {
        this.name = name;
        // 见PluggableEngineManager.PluggableEngineService中的注释
        StorageEngineManager.getInstance().registerEngine(this);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public void close() {
    }

    @Override
    public LobStorage getLobStorage(DataHandler dataHandler, Storage storage) {
        throw DbException.getUnsupportedException("getLobStorage");
    }
}
