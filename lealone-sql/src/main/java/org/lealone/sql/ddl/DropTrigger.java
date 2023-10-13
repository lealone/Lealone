/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.Right;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.TriggerObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP TRIGGER
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropTrigger extends SchemaStatement {

    private String triggerName;
    private boolean ifExists;

    public DropTrigger(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_TRIGGER;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TRIGGER, session);
        if (lock == null)
            return -1;

        TriggerObject trigger = schema.findTrigger(session, triggerName);
        if (trigger == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TRIGGER_NOT_FOUND_1, triggerName);
            }
        } else {
            Table table = trigger.getTable();
            session.getUser().checkRight(table, Right.ALL);
            schema.remove(session, trigger, lock);
        }
        return 0;
    }
}
