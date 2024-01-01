/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.sql;

import com.lealone.db.session.ServerSession;
import com.lealone.plugins.mysql.server.MySQLServerEngine;
import com.lealone.sql.SQLEngineBase;
import com.lealone.sql.SQLParserBase;

public class MySQLEngine extends SQLEngineBase {

    public MySQLEngine() {
        super(MySQLServerEngine.NAME);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return MySQLParser.quoteIdentifier(identifier);
    }

    @Override
    public SQLParserBase createParser(ServerSession session) {
        return new MySQLParser(session);
    }
}
