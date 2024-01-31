/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.util.Utils;

//4大引擎的标记接口
public interface PluggableEngine extends Plugin {

    public static <PE extends PluggableEngine> PE getEngine(Class<PE> engineClass, String engineType,
            String engineName) {
        try {
            PE pe = PluginManager.getPlugin(engineClass, engineName);
            if (pe == null) {
                pe = Utils.newInstance(engineName);
                PluginManager.register(engineClass, pe, engineName);
            }
            return pe;
        } catch (Throwable e) {
            throw new ConfigException("Failed to register " + engineType + " engine: " + engineName, e);
        }
    }
}
