/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql;

import org.lealone.db.session.ServerSession;
import org.lealone.plugins.mysql.server.MySQLServerEngine;
import org.lealone.sql.SQLEngineBase;
import org.lealone.sql.SQLParserBase;

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
