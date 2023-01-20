/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.api.Trigger;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.TriggerObject;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * CREATE TRIGGER
 * 
 * @author H2 Group
 * @author zhh
 */
public class CreateTrigger extends SchemaStatement {

    private String triggerName;
    private boolean ifNotExists;
    private boolean insteadOf;
    private boolean before;
    private int typeMask;
    private boolean rowBased;
    private int queueSize = TriggerObject.DEFAULT_QUEUE_SIZE;
    private boolean noWait;
    private String tableName;
    private String triggerClassName;
    private String triggerSource;
    private boolean force;
    private boolean onRollback;

    public CreateTrigger(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.CREATE_TRIGGER;
    }

    public void setTriggerName(String name) {
        this.triggerName = name;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setInsteadOf(boolean insteadOf) {
        this.insteadOf = insteadOf;
    }

    public void setBefore(boolean before) {
        this.before = before;
    }

    public void setTypeMask(int typeMask) {
        this.typeMask = typeMask;
    }

    public void setRowBased(boolean rowBased) {
        this.rowBased = rowBased;
    }

    public void setQueueSize(int size) {
        this.queueSize = size;
    }

    public void setNoWait(boolean noWait) {
        this.noWait = noWait;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTriggerClassName(String triggerClassName) {
        this.triggerClassName = triggerClassName;
    }

    public void setTriggerSource(String triggerSource) {
        this.triggerSource = triggerSource;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public void setOnRollback(boolean onRollback) {
        this.onRollback = onRollback;
    }

    @Override
    public int update() {
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.TRIGGER, session);
        if (lock == null)
            return -1;

        if (schema.findTrigger(session, triggerName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.TRIGGER_ALREADY_EXISTS_1, triggerName);
        }
        if ((typeMask & Trigger.SELECT) == Trigger.SELECT && rowBased) {
            throw DbException.get(ErrorCode.TRIGGER_SELECT_AND_ROW_BASED_NOT_SUPPORTED, triggerName);
        }
        int id = getObjectId();
        Table table = schema.getTableOrView(session, tableName);
        TriggerObject trigger = new TriggerObject(schema, id, triggerName, table);
        trigger.setInsteadOf(insteadOf);
        trigger.setBefore(before);
        trigger.setNoWait(noWait);
        trigger.setQueueSize(queueSize);
        trigger.setRowBased(rowBased);
        trigger.setTypeMask(typeMask);
        trigger.setOnRollback(onRollback);
        if (this.triggerClassName != null) {
            trigger.setTriggerClassName(triggerClassName, force);
        } else {
            trigger.setTriggerSource(triggerSource, force);
        }
        lock.addHandler(ar -> {
            if (ar.isSucceeded())
                table.addTrigger(trigger);
        });
        schema.add(session, trigger, lock);
        return 0;
    }
}
