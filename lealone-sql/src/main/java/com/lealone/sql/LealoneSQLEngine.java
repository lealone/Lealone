/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.db.Constants;
import com.lealone.db.session.ServerSession;

public class LealoneSQLEngine extends SQLEngineBase {

    public LealoneSQLEngine() {
        super(Constants.DEFAULT_SQL_ENGINE_NAME);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return LealoneSQLParser.quoteIdentifier(identifier);
    }

    @Override
    public SQLParserBase createParser(ServerSession session) {
        return new LealoneSQLParser(session);
    }
}
