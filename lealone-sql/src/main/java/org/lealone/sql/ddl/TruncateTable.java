/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.CommandInterface;
import org.lealone.db.Right;
import org.lealone.db.Session;
import org.lealone.db.table.Table;

/**
 * This class represents the statement
 * TRUNCATE TABLE
 */
public class TruncateTable extends DefineCommand {

    private Table table;

    public TruncateTable(Session session) {
        super(session);
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        session.commit(true);
        if (!table.canTruncate()) {
            throw DbException.get(ErrorCode.CANNOT_TRUNCATE_1, table.getSQL());
        }
        session.getUser().checkRight(table, Right.DELETE);
        table.lock(session, true, true);
        table.truncate(session);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.TRUNCATE_TABLE;
    }

}
