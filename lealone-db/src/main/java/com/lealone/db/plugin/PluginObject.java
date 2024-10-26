/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.plugin;

import java.io.IOException;
import java.net.URLClassLoader;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.MapUtils;
import com.lealone.common.util.StatementBuilder;
import com.lealone.db.Database;
import com.lealone.db.DbObjectBase;
import com.lealone.db.DbObjectType;

/**
 * Represents a database plugin object.
 */
public class PluginObject extends DbObjectBase {

    private final String implementBy;
    private final String classPath;
    private final CaseInsensitiveMap<String> parameters;
    private Plugin plugin;
    private URLClassLoader classLoader;
    private String state = "inited";

    public PluginObject(Database database, int id, String pluginName, String implementBy,
            String classPath, CaseInsensitiveMap<String> parameters) {
        super(database, id, pluginName);
        this.implementBy = implementBy;
        this.classPath = classPath;
        this.parameters = parameters;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.PLUGIN;
    }

    public Plugin getPlugin() {
        return plugin;
    }

    public void setPlugin(Plugin plugin) {
        this.plugin = plugin;
    }

    public URLClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(URLClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public void close() {
        plugin.close();
        if (classLoader != null) {
            try {
                classLoader.close();
            } catch (IOException e) {
            }
        }
    }

    @Override
    public String getCreateSQL() {
        StatementBuilder sql = new StatementBuilder("CREATE PLUGIN ");
        sql.append("IF NOT EXISTS ");
        sql.append(getSQL());
        sql.append(" IMPLEMENT BY '").append(implementBy).append("'");
        if (classPath != null) {
            sql.append(" CLASS PATH '").append(classPath).append("'");
        }
        if (parameters != null && !parameters.isEmpty()) {
            sql.append(" PARAMETERS");
            Database.appendMap(sql, parameters);
        }
        return sql.toString();
    }

    public void start() {
        plugin.start();
        state = "started";
    }

    public void stop() {
        plugin.stop();
        state = "stopped";
    }

    public String getState() {
        return state;
    }

    public boolean isAutoStart() {
        return MapUtils.getBoolean(parameters, "auto_start", false);
    }
}
