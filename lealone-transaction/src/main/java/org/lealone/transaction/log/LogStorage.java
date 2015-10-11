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
package org.lealone.transaction.log;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.storage.StorageMap;
import org.lealone.storage.fs.FilePath;
import org.lealone.storage.fs.FileUtils;
import org.lealone.storage.type.DataType;

/**
 * A log storage
 * 
 * @author zhh
 */
public class LogStorage {

    public static final char MAP_NAME_ID_SEPARATOR = '-';

    private static final CopyOnWriteArrayList<LogMap<?, ?>> logMaps = new CopyOnWriteArrayList<>();

    private final ConcurrentHashMap<String, StorageMap<?, ?>> maps = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> ids = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> names = new ConcurrentHashMap<>();
    private final Map<String, Object> config;
    private final LogStorageBackgroundThread backgroundThread;

    private int lastMapId;

    LogStorage(Map<String, Object> config) {
        this.config = config;
        if (!config.containsKey("inMemory")) {
            String storageName = (String) config.get("storageName");
            if (storageName != null) {
                if (!FileUtils.exists(storageName))
                    FileUtils.createDirectories(storageName);

                FilePath dir = FilePath.get(storageName);
                for (FilePath fp : dir.newDirectoryStream()) {
                    String mapFullName = fp.getName();
                    int mapIdStartPos = mapFullName.lastIndexOf(MAP_NAME_ID_SEPARATOR);
                    if (mapIdStartPos > 0) {
                        String mapName = mapFullName.substring(0, mapIdStartPos);
                        int mapId = Integer.parseInt(mapFullName.substring(mapIdStartPos + 1));
                        if (mapId > lastMapId)
                            lastMapId = mapId;
                        ids.put(mapName, mapId);
                        names.put(mapId, mapName);
                    }
                }
            }
        }

        backgroundThread = new LogStorageBackgroundThread(this);
    }

    public <K, V> LogMap<K, V> openBufferedMap(String name, DataType keyType, DataType valueType) {
        LogMap<K, V> m = new LogMap<>(++lastMapId, name, keyType, valueType, config);
        logMaps.add(m);
        return m;
    }

    public String getMapName(int id) {
        return names.get(id);
    }

    public synchronized void close() {
        backgroundThread.close();

        for (StorageMap<?, ?> map : maps.values())
            map.close();

        maps.clear();
        ids.clear();
    }

    public synchronized void commit() {
        for (StorageMap<?, ?> map : maps.values())
            map.save();
    }

    public Set<String> getMapNames() {
        return new HashSet<String>(maps.keySet());
    }

    public Collection<StorageMap<?, ?>> getMaps() {
        return maps.values();
    }

    private static class LogStorageBackgroundThread extends Thread {
        private final int sleep;
        private boolean running = true;

        LogStorageBackgroundThread(LogStorage storage) {
            super("LogStorageBackgroundThread");
            this.sleep = 1000;
            setDaemon(true);
        }

        void close() {
            running = false;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    sleep(sleep);
                } catch (InterruptedException e) {
                    continue;
                }

                for (LogMap<?, ?> map : LogStorage.logMaps) {
                    map.save();
                }
            }
        }
    }
}
