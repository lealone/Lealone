/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import java.net.URL;
import java.net.URLClassLoader;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.Utils;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.Plugin;
import org.lealone.db.PluginObject;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

public class CreatePlugin extends AdminStatement {

    private String pluginName;
    private String implementBy;
    private String classPath;
    private boolean ifNotExists;
    private CaseInsensitiveMap<String> parameters;

    public CreatePlugin(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_PLUGIN;
    }

    public void setPluginName(String name) {
        this.pluginName = name;
    }

    public void setImplementBy(String implementBy) {
        this.implementBy = implementBy;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setParameters(CaseInsensitiveMap<String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "create plugin");
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusivePluginLock(session);
        if (lock == null)
            return -1;
        PluginObject pluginObject = lealoneDB.findPluginObject(session, pluginName);
        if (pluginObject != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.PLUGIN_ALREADY_EXISTS_1, pluginName);
        }
        Plugin plugin = null;
        URLClassLoader cl = null;
        if (classPath != null) {
            String[] a = classPath.split(",");
            URL[] urls = new URL[a.length];
            for (int i = 0; i < a.length; i++) {
                urls[i] = Utils.toURL(a[i]);
            }
            cl = new URLClassLoader(urls);
            try {
                plugin = Utils.newInstance(cl.loadClass(implementBy));
            } catch (Throwable t) {
                try {
                    cl.close();
                } catch (Exception e) {
                }
                throw DbException.convert(t);
            }
        } else {
            plugin = Utils.newInstance(implementBy);
        }
        if (parameters == null)
            parameters = new CaseInsensitiveMap<>();
        plugin.init(parameters);

        int id = getObjectId(lealoneDB);
        pluginObject = new PluginObject(lealoneDB, id, pluginName, implementBy, classPath, parameters);
        pluginObject.setPlugin(plugin);
        pluginObject.setClassLoader(cl);
        lealoneDB.addDatabaseObject(session, pluginObject, lock);
        // 将缓存过期掉
        lealoneDB.getNextModificationMetaId();
        return 0;
    }
}
