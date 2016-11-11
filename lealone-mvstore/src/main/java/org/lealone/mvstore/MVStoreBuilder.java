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
package org.lealone.mvstore;

import java.util.HashMap;

import org.lealone.storage.StorageBuilder;

public class MVStoreBuilder extends StorageBuilder {

    private static HashMap<String, MVStore> cache = new HashMap<>();

    /**
     * Open the storage.
     * 
     * @return the opened storage
     */
    @Override
    public MVStore openStorage() {
        String storageName = (String) config.get("storageName");
        MVStore store = cache.get(storageName);
        if (store == null) {
            synchronized (cache) {
                store = cache.get(storageName);
                if (store == null) {
                    store = new MVStore(config);
                    cache.put(storageName, store);
                }
            }
        }
        return store;
    }

    @Override
    public StorageBuilder storageName(String storageName) {
        if (storageName != null)
            set("fileName", storageName + Constants.SUFFIX_MV_FILE);
        return super.storageName(storageName);
    }

}
