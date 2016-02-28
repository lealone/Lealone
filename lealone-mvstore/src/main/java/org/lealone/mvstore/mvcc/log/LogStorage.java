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
package org.lealone.mvstore.mvcc.log;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.db.Constants;
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

    public static String LOG_SYNC_TYPE_PERIODIC = "periodic";
    public static String LOG_SYNC_TYPE_BATCH = "batch";
    public static String LOG_SYNC_TYPE_NO_SYNC = "no_sync";

    public static final char MAP_NAME_ID_SEPARATOR = Constants.NAME_SEPARATOR;

    private static final String TEMP_MAP_NAME_PREFIX = "temp" + MAP_NAME_ID_SEPARATOR;

    private static final ConcurrentHashMap<String, ConcurrentSkipListSet<Integer>> ids = new ConcurrentHashMap<>();

    static final CopyOnWriteArrayList<LogMap<?, ?>> logMaps = new CopyOnWriteArrayList<>();

    static LogMap<?, ?> redoLog;

    private final Map<String, String> config;

    public final LogSyncService logSyncService;

    /**
     * The next id of a temporary map.
     */
    private int nextTempMapId;

    public LogStorage(Map<String, String> config) {
        this.config = config;
        String baseDir = config.get("base_dir");
        String logDir = config.get("transaction_log_dir");
        String storageName = baseDir + File.separator + logDir;
        config.put("storageName", storageName);

        if (!FileUtils.exists(storageName))
            FileUtils.createDirectories(storageName);

        FilePath dir = FilePath.get(storageName);
        for (FilePath fp : dir.newDirectoryStream()) {
            String mapFullName = fp.getName();
            if (mapFullName.startsWith(TEMP_MAP_NAME_PREFIX)) {
                fp.delete();
                continue;
            }

            int mapIdStartPos = mapFullName.lastIndexOf(MAP_NAME_ID_SEPARATOR);
            if (mapIdStartPos > 0) {
                String mapName = mapFullName.substring(0, mapIdStartPos);
                int mapId = Integer.parseInt(mapFullName.substring(mapIdStartPos + 1));
                addMapId(mapName, mapId);
            }
        }
        String logSyncType = config.get("log_sync_type");
        if (logSyncType == null || LOG_SYNC_TYPE_PERIODIC.equalsIgnoreCase(logSyncType))
            logSyncService = new PeriodicLogSyncService(config);
        else if (LOG_SYNC_TYPE_BATCH.equalsIgnoreCase(logSyncType))
            logSyncService = new BatchLogSyncService(config);
        else if (LOG_SYNC_TYPE_NO_SYNC.equalsIgnoreCase(logSyncType))
            logSyncService = new NoLogSyncService();
        else
            throw new IllegalArgumentException("Unknow log_sync_type: " + logSyncType);

        logSyncService.start();
    }

    public synchronized StorageMap<Object, Integer> createTempMap() {
        String mapName = LogStorage.TEMP_MAP_NAME_PREFIX + (++nextTempMapId);
        return openLogMap(mapName, null, null);
    }

    public <K, V> LogMap<K, V> openLogMap(String name, DataType keyType, DataType valueType) {
        int mapId = 1;
        if (ids.containsKey(name))
            mapId = ids.get(name).last();
        LogMap<K, V> m = new LogMap<>(mapId, name, keyType, valueType, config);
        logMaps.add(m);
        if ("redoLog".equals(name))
            redoLog = m;
        return m;
    }

    public synchronized void close() {
        for (StorageMap<?, ?> map : logMaps)
            map.save();

        if (logSyncService != null) {
            logSyncService.close();
            try {
                logSyncService.join();
            } catch (InterruptedException e) {
            }
        }

        for (StorageMap<?, ?> map : logMaps)
            map.close();

        logMaps.clear();
        ids.clear();
    }

    public static Integer getPreviousId(String name, Integer currentId) {
        Integer id = null;
        if (ids.containsKey(name))
            id = ids.get(name).lower(currentId);
        return id;
    }

    public static void addMapId(String mapName, Integer mapId) {
        ConcurrentSkipListSet<Integer> set = ids.get(mapName);
        if (set == null) {
            set = new ConcurrentSkipListSet<Integer>();
            ids.putIfAbsent(mapName, set);
            set = ids.get(mapName);
        }
        set.add(mapId);
    }
}
