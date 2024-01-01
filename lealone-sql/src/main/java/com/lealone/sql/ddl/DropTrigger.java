/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.auth.Right;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.TriggerObject;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;
import com.lealone.sql.SQLStatement;

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
