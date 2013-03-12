/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.lealone.command.ddl;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.dbobject.Right;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.TriggerObject;
import com.codefollower.lealone.dbobject.table.Table;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.message.DbException;

/**
 * This class represents the statement
 * DROP TRIGGER
 */
public class DropTrigger extends SchemaCommand {

    private String triggerName;
    private boolean ifExists;

    public DropTrigger(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setTriggerName(String triggerName) {
        this.triggerName = triggerName;
    }

    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        TriggerObject trigger = getSchema().findTrigger(triggerName);
        if (trigger == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.TRIGGER_NOT_FOUND_1, triggerName);
            }
        } else {
            Table table = trigger.getTable();
            session.getUser().checkRight(table, Right.ALL);
            db.removeSchemaObject(session, trigger);
        }
        return 0;
    }

    public int getType() {
        return CommandInterface.DROP_TRIGGER;
    }

}
