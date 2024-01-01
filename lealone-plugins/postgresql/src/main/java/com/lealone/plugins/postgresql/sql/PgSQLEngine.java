/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.sql;

import com.lealone.db.session.ServerSession;
import com.lealone.plugins.postgresql.server.PgServerEngine;
import com.lealone.sql.SQLEngineBase;

public class PgSQLEngine extends SQLEngineBase {

    public PgSQLEngine() {
        super(PgServerEngine.NAME);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return PgSQLParser.quoteIdentifier(identifier);
    }

    @Override
    public PgSQLParser createParser(ServerSession session) {
        return new PgSQLParser(session);
    }
}
