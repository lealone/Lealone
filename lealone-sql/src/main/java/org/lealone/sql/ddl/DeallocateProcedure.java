/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.ServerSession;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DEALLOCATE
 */
public class DeallocateProcedure extends DefineStatement {

    private String procedureName;

    public DeallocateProcedure(ServerSession session) {
        super(session);
    }

    @Override
    public int update() {
        session.removeProcedure(procedureName);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    @Override
    public int getType() {
        return SQLStatement.DEALLOCATE;
    }

}
