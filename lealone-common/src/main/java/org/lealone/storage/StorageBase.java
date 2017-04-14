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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.db.Constants;

public abstract class StorageBase implements Storage {

    protected static final String TEMP_NAME_PREFIX = "_temp_" + Constants.NAME_SEPARATOR;

    protected final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();

    protected boolean closed;

    @Override
    public boolean hasMap(String name) {
        return maps.containsKey(name);
    }

    @Override
    public String nextTemporaryMapName() {
        int i = 0;
        String name = null;
        while (true) {
            name = TEMP_NAME_PREFIX + i;
            if (!maps.containsKey(name))
                return name;
        }
    }

    @Override
    public void backupTo(String fileName) {
    }

    @Override
    public void save() {
        for (StorageMap<?, ?> map : maps.values())
            map.save();
    }

    @Override
    public void close() {
        closeImmediately();
    }

    @Override
    public void closeImmediately() {
        closed = true;

        for (StorageMap<?, ?> map : maps.values())
            map.close();

        maps.clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    public Set<String> getMapNames() {
        return new HashSet<String>(maps.keySet());
    }

    public void closeMap(String name) {
        maps.remove(name);
    }
}
