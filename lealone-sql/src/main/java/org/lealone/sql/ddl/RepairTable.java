/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * REPAIR TABLE
 */
public class RepairTable extends SchemaStatement {

    private Table table;

    public RepairTable(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.REPAIR_TABLE;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        table.repair(session);
        return 0;
    }
}
