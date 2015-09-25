/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.schema;

import org.lealone.common.message.DbException;
import org.lealone.common.message.Trace;
import org.lealone.common.value.Value;
import org.lealone.db.DbObject;
import org.lealone.db.Session;
import org.lealone.db.expression.Expression;
import org.lealone.db.expression.ValueExpression;
import org.lealone.db.table.Table;

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

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError();
    }

    @Override
    public String getDropSQL() {
        return null;
    }

    @Override
    public String getCreateSQL() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }

    @Override
    public int getType() {
        return DbObject.CONSTANT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        invalidate();
    }

    @Override
    public void checkRename() {
        // ok
    }

    public void setValue(Value value) {
        this.value = value;
        expression = ValueExpression.get(value);
    }

    public Expression getValue() {
        return expression;
    }

}
