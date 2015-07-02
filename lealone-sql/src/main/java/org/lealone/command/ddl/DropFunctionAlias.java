/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.api.ErrorCode;
import org.lealone.command.CommandInterface;
import org.lealone.dbobject.FunctionAlias;
import org.lealone.dbobject.Schema;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

/**
 * This class represents the statement
 * DROP ALIAS
 */
public class DropFunctionAlias extends SchemaCommand {

    private String aliasName;
    private boolean ifExists;

    public DropFunctionAlias(Session session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        FunctionAlias functionAlias = getSchema().findFunction(aliasName);
        if (functionAlias == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1, aliasName);
            }
        } else {
            db.removeSchemaObject(session, functionAlias);
        }
        return 0;
    }

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_ALIAS;
    }

}
