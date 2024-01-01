/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.db.session.ServerSession;

public class LealoneSQLParser extends SQLParserBase {

    public LealoneSQLParser(ServerSession session) {
        super(session);
    }
}