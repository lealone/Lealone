/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.dbobject;

import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.ValueExpression;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.message.Trace;
import com.codefollower.yourbase.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 */
public class Constant extends SchemaObjectBase {

    private Value value;
    private ValueExpression expression;

    public Constant(Schema schema, int id, String name) {
        initSchemaObjectBase(schema, id, name, Trace.SCHEMA);
    }

    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError();
    }

    public String getDropSQL() {
        return null;
    }

    public String getCreateSQL() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }

    public int getType() {
        return DbObject.CONSTANT;
    }

    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    public void checkRename() {
        // ok
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }

    public ValueExpression getValue() {
        return expression;
    }

}
