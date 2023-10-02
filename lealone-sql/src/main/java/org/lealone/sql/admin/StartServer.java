/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluginManager;
import org.lealone.db.session.ServerSession;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * START SERVER
 */
// TODO 还未完全实现
public class StartServer extends AdminStatement {

    private final String name;
    private final CaseInsensitiveMap<String> parameters;

    public StartServer(ServerSession session, String name, CaseInsensitiveMap<String> parameters) {
        super(session);
        this.name = name;
        if (parameters == null)
            parameters = new CaseInsensitiveMap<>();
        this.parameters = parameters;
    }

    @Override
    public int getType() {
        return SQLStatement.START_SERVER;
    }

    @Override
    public int update() {
        LealoneDatabase.checkAdminRight(session, "start server");
        for (ProtocolServerEngine e : PluginManager.getPlugins(ProtocolServerEngine.class)) {
            if (e.getName().equalsIgnoreCase(name)) {
                CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>();
                if (e.getConfig() != null) {
                    parameters.putAll(e.getConfig());
                }
                parameters.putAll(this.parameters);
                if (!e.isInited())
                    e.init(parameters);
                ProtocolServer server = e.getProtocolServer();
                if (!server.isStarted()) {
                    server.start();
                }
            }
        }
        return 0;
    }
}
