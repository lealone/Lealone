/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.sql.SQLStatement;

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
    public boolean isIfDDL() {
        return ifExists;
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
