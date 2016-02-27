/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.common.exceptions.DbException;
import org.lealone.db.Database;
import org.lealone.db.ServerSession;
import org.lealone.db.auth.Right;
import org.lealone.db.index.Index;
import org.lealone.db.schema.Schema;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * ALTER INDEX RENAME
 */
public class AlterIndexRename extends DefineStatement {

    private Index oldIndex;
    private String newIndexName;

    public AlterIndexRename(ServerSession session) {
        super(session);
    }

    @Override
    public int getType() {
        return SQLStatement.ALTER_INDEX_RENAME;
    }

    public void setOldIndex(Index index) {
        oldIndex = index;
    }

    public void setNewName(String name) {
        newIndexName = name;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        Schema schema = oldIndex.getSchema();
        if (schema.findIndex(session, newIndexName) != null || newIndexName.equals(oldIndex.getName())) {
            throw DbException.get(ErrorCode.INDEX_ALREADY_EXISTS_1, newIndexName);
        }
        session.getUser().checkRight(oldIndex.getTable(), Right.ALL);
        db.renameSchemaObject(session, oldIndex, newIndexName);
        return 0;
    }

}
