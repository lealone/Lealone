/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.schema.Sequence;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP SEQUENCE
 * 
 * @author H2 Group
 * @author zhh
 */
public class DropSequence extends SchemaStatement {

    private String sequenceName;
    private boolean ifExists;

    public DropSequence(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_SEQUENCE;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.SEQUENCE, session);
        if (lock == null)
            return -1;

        Sequence sequence = schema.findSequence(session, sequenceName);
        if (sequence == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
            }
        } else {
            if (sequence.getBelongsToTable()) {
                throw DbException.get(ErrorCode.SEQUENCE_BELONGS_TO_A_TABLE_1, sequenceName);
            }
            schema.remove(session, sequence, lock);
        }
        return 0;
    }
}
