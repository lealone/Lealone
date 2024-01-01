/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.ddl;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.DbObjectType;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.lock.DbObjectLock;
import com.lealone.db.schema.Schema;
import com.lealone.db.service.Service;
import com.lealone.db.session.ServerSession;
import com.lealone.sql.SQLStatement;

/**
 * This class represents the statement
 * DROP SERVICE
 */
public class DropService extends SchemaStatement {

    private String serviceName;
    private boolean ifExists;

    public DropService(ServerSession session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int getType() {
        return SQLStatement.DROP_SERVICE;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        DbObjectLock lock = schema.tryExclusiveLock(DbObjectType.SERVICE, session);
        if (lock == null)
            return -1;

        Service service = schema.findService(session, serviceName);
        if (service == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SERVICE_NOT_FOUND_1, serviceName);
            }
        } else {
            schema.remove(session, service, lock);
        }
        return 0;
    }
}
