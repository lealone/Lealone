/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.admin;

import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.session.ServerSession;
import com.lealone.server.ProtocolServerEngine;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * START SERVER
 */
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
        ProtocolServerEngine e = ShutdownServer.getProtocolServerEngine(name);
        if (!e.isInited())
            e.init(parameters);
        e.start();
        return 0;
    }
}
