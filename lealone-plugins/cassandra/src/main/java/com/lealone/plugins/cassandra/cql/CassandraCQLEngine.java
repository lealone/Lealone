/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.cassandra.cql;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLEngineBase;

import com.lealone.plugins.cassandra.server.CassandraServerEngine;

public class CassandraCQLEngine extends SQLEngineBase {

    public CassandraCQLEngine() {
        super(CassandraServerEngine.NAME);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return CassandraCQLParser.quoteIdentifier(identifier);
    }

    @Override
    public CassandraCQLParser createParser(ServerSession session) {
        return new CassandraCQLParser(session);
    }
}
