/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.Right;
import org.lealone.dbobject.Schema;
import org.lealone.dbobject.index.Index;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * ALTER INDEX RENAME
 */
public class AlterIndexRename extends DefineCommand {

    private Index oldIndex;
    private String newIndexName;

    public AlterIndexRename(Session session) {
        super(session);
    }

    public void setOldIndex(Index index) {
        oldIndex = index;
    }

    public void setNewName(String name) {
        newIndexName = name;
    }

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

    public int getType() {
        return CommandInterface.ALTER_INDEX_RENAME;
    }

}
