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
package org.lealone.client.storage;

import java.util.Map;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.session.Session;
import org.lealone.storage.StorageBase;
import org.lealone.storage.type.StorageDataType;

public class ClientStorage extends StorageBase {

    private Session session;

    public ClientStorage() {
        super(null);
    }

    public ClientStorage(String url) {
        super(null);

        ConnectionInfo ci = new ConnectionInfo(url);
        session = ci.createSession().connect();
    }

    public Session getSession() {
        return session;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> ClientStorageMap<K, V> openMap(String name, StorageDataType keyType, StorageDataType valueType,
            Map<String, String> parameters) {
        ClientStorageMap<K, V> map = (ClientStorageMap<K, V>) maps.get(name);
        if (map == null) {
            synchronized (this) {
                map = (ClientStorageMap<K, V>) maps.get(name);
                if (map == null) {
                    map = new ClientStorageMap<>(name, keyType, valueType, this);
                    maps.put(name, map);
                }
            }
        }
        return map;
    }

    @Override
    public String getStoragePath() {
        return null;
    }
}
