/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.db.session.ServerSession;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.server.ProtocolServerEngineManager;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ADMIN SHUTDOWN SERVER
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
        session.getUser().checkAdmin();
        ConcurrentUtils.submitTask("ShutdownServerThread", () -> {
            try {
                Thread.sleep(1000); // 返回结果给客户端需要一点时间，如果立刻关闭网络连接就不能发送结果了
            } catch (InterruptedException e) {
            }
            for (ProtocolServerEngine e : ProtocolServerEngineManager.getInstance().getEngines()) {
                // 没有初始化的不用管
                if (e.isInited()) {
                    ProtocolServer server = e.getProtocolServer();
                    if (server.getPort() == port && server.isStarted()) {
                        server.stop();
                    }
                }
            }
        });
        return 0;
    }
}
