/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluginObject;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

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
        return 0;
    }
}
