/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.main.config;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import com.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import com.lealone.db.Constants;

public class Config {

    public String base_dir = "." + File.separator + Constants.PROJECT_NAME + "_data";

    public String listen_address = "127.0.0.1";

    public List<PluggableEngineDef> storage_engines;
    public List<PluggableEngineDef> transaction_engines;
    public List<PluggableEngineDef> sql_engines;
    public List<PluggableEngineDef> protocol_server_engines;

    public ServerEncryptionOptions server_encryption_options;
    public ClientEncryptionOptions client_encryption_options;

    public SchedulerDef scheduler;

    public Config() {
    }

    public Config(boolean isDefault) {
        if (!isDefault)
            return;
        storage_engines = new ArrayList<>(1);
        storage_engines.add(createEngineDef(Constants.DEFAULT_STORAGE_ENGINE_NAME, true, true));

        transaction_engines = new ArrayList<>(1);
        transaction_engines.add(createEngineDef(Constants.DEFAULT_TRANSACTION_ENGINE_NAME, true, true));

        sql_engines = new ArrayList<>(1);
        sql_engines.add(createEngineDef(Constants.DEFAULT_SQL_ENGINE_NAME, true, true));

        protocol_server_engines = new ArrayList<>(1);
        protocol_server_engines.add(createEngineDef("TCP", true, false));

        scheduler = new SchedulerDef();
        scheduler.parameters.put("scheduler_count", Runtime.getRuntime().availableProcessors() + "");
        mergeSchedulerParametersToEngines();
    }

    private static PluggableEngineDef createEngineDef(String name, boolean enabled, boolean isDefault) {
        PluggableEngineDef e = new PluggableEngineDef();
        e.name = name;
        e.enabled = enabled;
        e.is_default = isDefault;
        return e;
    }

    // 合并scheduler的参数到以下两种引擎，会用到
    public void mergeSchedulerParametersToEngines() {
        for (PluggableEngineDef e : protocol_server_engines) {
            if (e.enabled)
                e.parameters.putAll(scheduler.parameters);
        }
        for (PluggableEngineDef e : transaction_engines) {
            if (e.enabled)
                e.parameters.putAll(scheduler.parameters);
        }
    }

    public void mergeProtocolServerParameters(String name, String host, String port) {
        if (host == null && port == null)
            return;
        Map<String, String> parameters = getProtocolServerParameters(name);
        if (host != null)
            parameters.put("host", host);
        if (port != null)
            parameters.put("port", port);
    }

    public Map<String, String> getProtocolServerParameters(String name) {
        if (protocol_server_engines != null) {
            for (PluggableEngineDef def : protocol_server_engines) {
                if (name.equalsIgnoreCase(def.name)) {
                    return def.parameters;
                }
            }
        }
        return new HashMap<>(0);
    }

    public static String getProperty(String key) {
        return getProperty(key, null);
    }

    public static String getProperty(String key, String def) {
        return System.getProperty(Constants.PROJECT_NAME_PREFIX + key, def);
    }

    public static void setProperty(String key, String value) {
        System.setProperty(Constants.PROJECT_NAME_PREFIX + key, value);
    }

    public static Config getDefaultConfig() {
        return new Config(true);
    }

    public static Config mergeDefaultConfig(Config c) { // c是custom config
        Config d = getDefaultConfig();
        if (c == null)
            return d;
        c.storage_engines = mergeEngines(c.storage_engines, d.storage_engines);
        c.transaction_engines = mergeEngines(c.transaction_engines, d.transaction_engines);
        c.sql_engines = mergeEngines(c.sql_engines, d.sql_engines);
        c.protocol_server_engines = mergeEngines(c.protocol_server_engines, d.protocol_server_engines);

        c.scheduler = mergeMap(d.scheduler, c.scheduler);
        c.mergeSchedulerParametersToEngines();
        return c;
    }

    private static List<PluggableEngineDef> mergeEngines(List<PluggableEngineDef> newList,
            List<PluggableEngineDef> defaultList) {
        if (defaultList == null)
            return newList;
        if (newList == null)
            return defaultList;
        LinkedHashMap<String, PluggableEngineDef> map = new LinkedHashMap<>();
        for (PluggableEngineDef e : defaultList) {
            map.put(e.name.toUpperCase(), e);
        }
        for (PluggableEngineDef e : newList) {
            String name = e.name.toUpperCase();
            PluggableEngineDef defaultE = map.get(name);
            if (defaultE == null) {
                map.put(name, e);
            } else {
                defaultE.enabled = e.enabled;
                defaultE.parameters.putAll(e.parameters);
            }
        }
        return new ArrayList<>(map.values());
    }

    private static <T extends MapPropertyTypeDef> T mergeMap(T defaultMap, T newMap) {
        if (defaultMap == null)
            return newMap;
        if (newMap == null)
            return defaultMap;
        defaultMap.parameters.putAll(newMap.parameters);
        return defaultMap;
    }

    public static abstract class MapPropertyTypeDef {
        public String name;
        public Map<String, String> parameters = new HashMap<>();

        public MapPropertyTypeDef() {
        }

        public Map<String, String> getParameters() {
            return parameters;
        }

        public void setParameters(Map<String, String> parameters) {
            this.parameters = parameters;
        }
    }

    public static class SchedulerDef extends MapPropertyTypeDef {
    }

    public static class PluggableEngineDef extends MapPropertyTypeDef {
        public Boolean enabled = true;
        public Boolean is_default = false;
    }
}
