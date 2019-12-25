/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;

/**
 * This class represents a non-transaction statement that involves a schema.
 */
public abstract class SchemaStatement extends DefinitionStatement {

    private final Schema schema;

    /**
     * Create a new statement.
     *
     * @param session the session
     * @param schema the schema
     */
    public SchemaStatement(ServerSession session, Schema schema) {
        super(session);
        this.schema = schema;
    }

    /**
     * Get the schema
     *
     * @return the schema
     */
    protected Schema getSchema() {
        return schema;
    }

}
