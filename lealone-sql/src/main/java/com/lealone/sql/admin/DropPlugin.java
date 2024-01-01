/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.admin;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.PluginObject;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

public class DropPlugin extends AdminStatement {

    private String pluginName;
    private boolean ifExists;

    public DropPlugin(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_PLUGIN;
    }

    public void setPluginName(String name) {
        this.pluginName = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "drop plugin");
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusivePluginLock(session);
        if (lock == null)
            return -1;
        PluginObject pluginObject = lealoneDB.findPluginObject(session, pluginName);
        if (pluginObject == null) {
            if (!ifExists)
                throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, pluginName);
        } else {
            pluginObject.close();
            lealoneDB.removeDatabaseObject(session, pluginObject, lock);
            // 将缓存过期掉
            lealoneDB.getNextModificationMetaId();
        }
        return 0;
    }
}
