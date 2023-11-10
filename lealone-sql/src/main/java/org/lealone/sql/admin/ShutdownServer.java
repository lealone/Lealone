/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.ThreadUtils;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluginManager;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * SHUTDOWN SERVER
 */
public class ShutdownServer extends AdminStatement {

    private final int port;
    private final String name;

    public ShutdownServer(ServerSession session, int port, String name) {
        super(session);
        this.port = port;
        this.name = name;
    }

    @Override
    public int getType() {
        return SQLStatement.SHUTDOWN_SERVER;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "shutdown server");
        // 通过指定的名称来关闭server
        if (name != null) {
            ProtocolServerEngine e = getProtocolServerEngine(name);
            e.stop();
        } else {
            // 通过指定的端口号来关闭server，如果端口号小于0就关闭所有server
            ThreadUtils.start("ShutdownServerThread-Port-" + port, () -> {
                for (ProtocolServer server : ProtocolServerEngine.startedServers) {
                    if (port < 0 || server.getPort() == port) {
                        server.stop();
                    }
                }
            });
        }
        return 0;
    }

    public static ProtocolServerEngine getProtocolServerEngine(String name) {
        ProtocolServerEngine e = PluginManager.getPlugin(ProtocolServerEngine.class, name);
        if (e == null) {
            throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, name);
        }
        return e;
    }
}
