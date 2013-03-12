/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Right;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.index.Index;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;

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
