/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.util.ThreadUtils;
import org.lealone.db.LealoneDatabase;
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

    public ShutdownServer(ServerSession session, int port) {
        super(session);
        this.port = port;
    }

    @Override
    public int getType() {
        return SQLStatement.SHUTDOWN_SERVER;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "shutdown server");
        ThreadUtils.start("ShutdownServerThread-Port-" + port, () -> {
            for (ProtocolServer server : ProtocolServerEngine.startedServers) {
                if (port < 0 || server.getPort() == port) {
                    server.stop();
                }
            }
        });
        return 0;
    }
}
