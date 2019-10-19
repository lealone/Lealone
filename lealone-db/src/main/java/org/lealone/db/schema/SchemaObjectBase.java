/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.schema;

import org.lealone.db.DbObjectBase;

/**
 * The base class for classes implementing SchemaObject.
 */
public abstract class SchemaObjectBase extends DbObjectBase implements SchemaObject {

    protected final Schema schema;

    /**
     * Initialize some attributes of this object.
     *
     * @param schema the schema
     * @param id the object id
     * @param name the object name
     */
    protected SchemaObjectBase(Schema schema, int id, String name) {
        super(schema.getDatabase(), id, name);
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public String getSQL() {
        return schema.getSQL() + "." + super.getSQL();
    }

}
