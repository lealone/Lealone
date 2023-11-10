/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.admin;

import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.session.ServerSession;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.sql.SQLStatement;

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
