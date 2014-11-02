/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.command.ddl;

import org.lealone.command.CommandInterface;
import org.lealone.constant.ErrorCode;
import org.lealone.dbobject.FunctionAlias;
import org.lealone.dbobject.Schema;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.util.StringUtils;

/**
 * This class represents the statement
 * CREATE ALIAS
 */
public class CreateFunctionAlias extends SchemaCommand {

    private String aliasName;
    private String javaClassMethod;
    private boolean deterministic;
    private boolean ifNotExists;
    private boolean force;
    private String source;

    public CreateFunctionAlias(Session session, Schema schema) {
        super(session, schema);
    }

    public int update() {
        session.commit(true);
        session.getUser().checkAdmin();
        Database db = session.getDatabase();
        if (getSchema().findFunction(aliasName) != null) {
            if (!ifNotExists) {
                throw DbException.get(ErrorCode.FUNCTION_ALIAS_ALREADY_EXISTS_1, aliasName);
            }
        } else {
            int id = getObjectId();
            FunctionAlias functionAlias;
            if (javaClassMethod != null) {
                functionAlias = FunctionAlias.newInstance(getSchema(), id, aliasName, javaClassMethod, force);
            } else {
                functionAlias = FunctionAlias.newInstanceFromSource(getSchema(), id, aliasName, source, force);
            }
            functionAlias.setDeterministic(deterministic);
            db.addSchemaObject(session, functionAlias);
        }
        return 0;
    }

    public void setAliasName(String name) {
        this.aliasName = name;
    }

    /**
     * Set the qualified method name after removing whitespace.
     *
     * @param method the qualified method name
     */
    public void setJavaClassMethod(String method) {
        this.javaClassMethod = StringUtils.replaceAll(method, " ", "");
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public int getType() {
        return CommandInterface.CREATE_ALIAS;
    }

}
