/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.schema;

import org.lealone.db.DbObjectType;
import org.lealone.db.value.Value;

/**
 * A user-defined constant as created by the SQL statement
 * CREATE CONSTANT
 *
 * @author H2 Group
 * @author zhh
 */
public class Constant extends SchemaObjectBase {

    private final Value value;

    public Constant(Schema schema, int id, String name, Value value) {
        super(schema, id, name);
        this.value = value;
    }

    @Override
    public DbObjectType getType() {
        return DbObjectType.CONSTANT;
    }

    public Value getValue() {
        return value;
    }

    @Override
    public String getCreateSQL() {
        return "CREATE CONSTANT " + getSQL() + " VALUE " + value.getSQL();
    }
}
