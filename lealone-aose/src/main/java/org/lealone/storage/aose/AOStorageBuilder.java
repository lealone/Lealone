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
package org.lealone.storage.aose;

import java.util.HashMap;
import java.util.Map;

import org.lealone.storage.PageOperationHandlerFactory;
import org.lealone.storage.StorageBuilder;

public class AOStorageBuilder extends StorageBuilder {

    private static final HashMap<String, AOStorage> cache = new HashMap<>();
    private final PageOperationHandlerFactory pohFactory;

    public AOStorageBuilder() {
        this(new HashMap<>(0));
    }

    public AOStorageBuilder(Map<String, String> defaultConfig) {
        pohFactory = PageOperationHandlerFactory.create(defaultConfig);
        if (defaultConfig != null)
            config.putAll(defaultConfig);
    }

    @Override
    public PageOperationHandlerFactory getPageOperationHandlerFactory() {
        return pohFactory;
    }

    @Override
    public AOStorage openStorage() {
        String storagePath = (String) config.get("storagePath");
        AOStorage storage = cache.get(storagePath);
        if (storage == null) {
            synchronized (cache) {
                storage = cache.get(storagePath);
                if (storage == null) {
                    storage = new AOStorage(config, pohFactory);
                    cache.put(storagePath, storage);
                }
            }
        }
        return storage;
    }
}
