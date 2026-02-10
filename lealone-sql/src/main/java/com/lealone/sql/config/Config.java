/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.config;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lealone.common.security.EncryptionOptions.ClientEncryptionOptions;
import com.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.MapUtils;
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

    public static PluggableEngineDef createEngineDef(String name, boolean enabled, boolean isDefault) {
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

    public void setParameters(CaseInsensitiveMap<String> parameters) {
        base_dir = MapUtils.getString(parameters, "base_dir", base_dir);
        int pos1 = base_dir.indexOf("${");
        if (pos1 >= 0) {
            int pos2 = base_dir.indexOf('}');
            String envName = base_dir.substring(pos1 + 2, pos2);
            base_dir = System.getenv(envName) + base_dir.substring(pos2 + 1);
        }
        listen_address = MapUtils.getString(parameters, "listen_address", listen_address);
        mergeSchedulerParametersToEngines();
    }

    public void setSchedulerParameters(CaseInsensitiveMap<String> schedulerParameters) {
        scheduler.parameters.putAll(schedulerParameters);
    }

    public void addStorageEngine(CaseInsensitiveMap<String> storageEngine) {
        mergeEngines(storageEngine, storage_engines);
    }

    public void addTransactionEngine(CaseInsensitiveMap<String> transactionEngine) {
        mergeEngines(transactionEngine, transaction_engines);
    }

    public void addSqlEngine(CaseInsensitiveMap<String> sqlEngine) {
        mergeEngines(sqlEngine, sql_engines);
    }

    public void addProtocolServerEngine(CaseInsensitiveMap<String> protocolServerEngine) {
        mergeEngines(protocolServerEngine, protocol_server_engines);
    }

    public static void mergeEngines(Map<String, String> engineMap,
            List<PluggableEngineDef> defaultList) {
        PluggableEngineDef def = null;
        String name = engineMap.get("name");
        for (PluggableEngineDef e : defaultList) {
            if (name.equalsIgnoreCase(e.name)) {
                def = e;
                break;
            }
        }
        if (def == null) {
            def = new PluggableEngineDef();
            def.parameters = engineMap;
            defaultList.add(def);
        } else {
            def.parameters.putAll(engineMap);
        }
        def.name = name;
        def.enabled = Boolean.parseBoolean(engineMap.get("enabled"));
        if (engineMap.containsKey("is_default"))
            def.is_default = Boolean.parseBoolean(engineMap.get("is_default"));
    }

    public static abstract class MapPropertyTypeDef {
        public String name;
        public Map<String, String> parameters = new CaseInsensitiveMap<>();

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
        public boolean enabled = true;
        public boolean is_default = false;
    }
}
