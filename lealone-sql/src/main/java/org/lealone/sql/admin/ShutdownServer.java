/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
