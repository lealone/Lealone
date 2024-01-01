/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DEALLOCATE
 */
public class DeallocateProcedure extends DefinitionStatement {

    private String procedureName;

    public DeallocateProcedure(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.DEALLOCATE;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    @Override
    public int update() {
        session.removeProcedure(procedureName);
        return 0;
    }
}
