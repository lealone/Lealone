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

public class ShutdownPlugin extends AdminStatement {

    private final String pluginName;

    public ShutdownPlugin(ServerSession session, String pluginName) {
        super(session);
        this.pluginName = pluginName;
    }

    @Override
    public int getType() {
        return SQLStatement.SHUTDOWN_PLUGIN;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "shutdown plugin");
        LealoneDatabase lealoneDB = LealoneDatabase.getInstance();
        DbObjectLock lock = lealoneDB.tryExclusivePluginLock(session);
        if (lock == null)
            return -1;
        PluginObject pluginObject = lealoneDB.findPluginObject(session, pluginName);
        if (pluginObject == null) {
            throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, pluginName);
        }
        pluginObject.stop();
        // 将缓存过期掉
        lealoneDB.getNextModificationMetaId();
        return 0;
    }
}
