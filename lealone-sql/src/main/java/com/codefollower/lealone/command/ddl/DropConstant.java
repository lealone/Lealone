/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Constant;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;

/**
 * This class represents the statement
 * DROP CONSTANT
 */
public class DropConstant extends SchemaCommand {

    private String constantName;
    private boolean ifExists;

    public DropConstant(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Constant constant = getSchema().findConstant(constantName);
        if (constant == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
            }
        } else {
            db.removeSchemaObject(session, constant);
        }
        return 0;
    }

    public int getType() {
        return CommandInterface.DROP_CONSTANT;
    }

}
