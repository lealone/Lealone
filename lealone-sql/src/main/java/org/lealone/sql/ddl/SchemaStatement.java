/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.db.DbObjectType;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;

/**
 * This class represents a non-transaction statement that involves a schema.
 */
public abstract class SchemaStatement extends DefinitionStatement {

    protected final Schema schema;

    /**
     * Create a new statement.
     *
     * @param session the session
     * @param schema the schema
     */
    public SchemaStatement(ServerSession session, Schema schema) {
        super(session);
        this.schema = schema;
        session.getUser().checkSystemSchema(session, schema);
    }

    /**
     * Get the schema
     *
     * @return the schema
     */
    protected Schema getSchema() {
        return schema;
    }

    protected DbObjectLock tryAlterTable(Table table) {
        // 先用schema级别的排它锁来避免其他事务也来执行Alter Table操作
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TABLE_OR_VIEW, session);
        if (lock == null)
            return null;
        // 再用table级别的共享锁来避免其他事务进行Drop Table操作，但是不阻止DML操作
        if (!table.trySharedLock(session))
            return null;

        return lock;
    }
}
