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
import org.lealone.db.DbObjectType;
import org.lealone.db.ServerSession;
import org.lealone.db.schema.FunctionAlias;
import org.lealone.db.schema.Schema;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP ALIAS
 */
public class DropFunctionAlias extends SchemaStatement {

    private String aliasName;
    private boolean ifExists;

    public DropFunctionAlias(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_ALIAS;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        synchronized (getSchema().getLock(DbObjectType.FUNCTION_ALIAS)) {
            FunctionAlias functionAlias = getSchema().findFunction(aliasName);
            if (functionAlias == null) {
                if (!ifExists) {
                    throw DbException.get(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1, aliasName);
                }
            } else {
                db.removeSchemaObject(session, functionAlias);
            }
        }
        return 0;
    }

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

}
