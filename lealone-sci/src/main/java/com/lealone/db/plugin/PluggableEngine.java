/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.plugin;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.util.Utils;
import com.lealone.server.ProtocolServerEngine;
import com.lealone.sql.SQLEngine;
import com.lealone.storage.StorageEngine;
import com.lealone.transaction.TransactionEngine;

//4大引擎的标记接口
public interface PluggableEngine extends Plugin {

    public static <PE extends PluggableEngine> PE getEngine(Class<PE> engineClass, String engineName) {
        try {
            PE pe = PluginManager.getPlugin(engineClass, engineName);
            if (pe == null) {
                pe = Utils.newInstance(engineName);
                PluginManager.register(engineClass, pe, engineName);
            }
            return pe;
        } catch (Throwable e) {
            throw new ConfigException(
                    "Failed to register " + getEngineType(engineClass) + " engine: " + engineName, e);
        }
    }

    public static <PE extends PluggableEngine> String getEngineType(Class<PE> engineClass) {
        if (engineClass == StorageEngine.class) {
            return "storage";
        } else if (engineClass == TransactionEngine.class) {
            return "transaction";
        } else if (engineClass == SQLEngine.class) {
            return "sql";
        } else if (engineClass == ProtocolServerEngine.class) {
            return "protocol server";
        } else {
            return engineClass.getSimpleName();
        }
    }
}
