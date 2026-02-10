/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.config;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;
import com.lealone.sql.ddl.DefinitionStatement;

/**
 * This class represents the statement
 * CREATE CONFIG
 */
public class CreateConfig extends DefinitionStatement {

    private final Config config = new Config();

    public CreateConfig(ServerSession session, String name) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.UNKNOWN;
    }

    @Override
    public int update() {
        return 0;
    }

    public Config getConfig() {
        return config;
    }
}
