/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.admin;

import com.lealone.common.util.ThreadUtils;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.session.ServerSession;
import com.lealone.server.ProtocolServer;
import com.lealone.server.ProtocolServerEngine;
import com.lealone.sql.SQLStatement;

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
        String threadName = "ShutdownServerThread-" + (name != null ? name : "port(" + port + ")");
        ThreadUtils.start(threadName, () -> {
            for (ProtocolServer server : ProtocolServerEngine.startedServers) {
                if (name != null) {
                    // 通过指定的名称来关闭server
                    if (name.equalsIgnoreCase(server.getType())
                            || name.equalsIgnoreCase(server.getName()))
                        server.stop();
                } else if (port < 0 || server.getPort() == port) {
                    // 通过指定的端口号来关闭server，如果端口号小于0就关闭所有server
                    server.stop();
                }
            }
        });
        return 0;
    }
}
