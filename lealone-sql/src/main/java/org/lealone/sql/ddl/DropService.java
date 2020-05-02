/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.sql.ddl;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.DbObjectType;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.service.Service;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.LockTable;
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
    public int update() {
        session.getUser().checkAdmin();
        LockTable lockTable = schema.tryExclusiveLock(DbObjectType.SERVICE, session);
        if (lockTable == null)
            return -1;

        Service service = schema.findService(session, serviceName);
        if (service == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SERVICE_NOT_FOUND_1, serviceName);
            }
        } else {
            schema.remove(session, service, lockTable);
        }
        return 0;
    }
}
