/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.ddl;

import com.lealone.db.session.ServerSession;

//只是一个标记类
public abstract class AuthStatement extends DefinitionStatement {

    protected AuthStatement(ServerSession session) {
        super(session);
    }
}
