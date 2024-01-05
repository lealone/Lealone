/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.cassandra.cql;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLParserBase;
import com.lealone.sql.StatementBase;
import com.lealone.sql.expression.Expression;

public class CassandraCQLParser extends SQLParserBase {

    public CassandraCQLParser(ServerSession session) {
        super(session);
    }

    @Override
    protected StatementBase parseStatement(char first) {
        StatementBase s = null;
        return s;
    }

    @Override
    protected Expression parseVariable() {
        return super.parseVariable();
    }
}
