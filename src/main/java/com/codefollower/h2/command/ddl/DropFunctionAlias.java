/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.h2.command.ddl;

import com.codefollower.h2.command.CommandInterface;
import com.codefollower.h2.constant.ErrorCode;
import com.codefollower.h2.engine.Database;
import com.codefollower.h2.engine.FunctionAlias;
import com.codefollower.h2.engine.Session;
import com.codefollower.h2.message.DbException;
import com.codefollower.h2.schema.Schema;

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

    public int getType() {
        return CommandInterface.DROP_ALIAS;
    }

}
