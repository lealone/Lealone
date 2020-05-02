/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.FunctionAlias;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.LockTable;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP ALIAS
 * 
 * @author H2 Group
 * @author zhh
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

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        LockTable lockTable = schema.tryExclusiveLock(DbObjectType.FUNCTION_ALIAS, session);
        if (lockTable == null)
            return -1;

        FunctionAlias functionAlias = schema.findFunction(session, aliasName);
        if (functionAlias == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_NOT_FOUND_1, aliasName);
            }
        } else {
            schema.remove(session, functionAlias, lockTable);
        }
        return 0;
    }
}
