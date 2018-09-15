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
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetEndpoint;
import org.lealone.sql.router.RouterHolder;

public abstract class DatabaseStatement extends DefineStatement {

    protected DatabaseStatement(ServerSession session) {
        super(session);
    }

    @Override
    public boolean isDatabaseStatement() {
        return true;
    }

    protected void checkRight() {
        checkRight(null);
    }

    protected void checkRight(Integer errorCode) {
        // 只有用管理员连接到LealoneDatabase才能执行CREATE/ALTER/DROP DATABASE语句
        if (!(LealoneDatabase.getInstance() == session.getDatabase() && session.getUser().isAdmin())) {
            if (errorCode != null)
                throw DbException.get(errorCode.intValue());
            else
                throw DbException.get(ErrorCode.GENERAL_ERROR_1,
                        "create/alter/drop database only allowed for the super user");
        }
    }

    protected boolean isTargetEndpoint(Database db) {
        NetEndpoint localEndpoint = NetEndpoint.getLocalTcpEndpoint();
        return db.isTargetEndpoint(localEndpoint);
    }

    protected void executeDatabaseStatement(Database db) {
        if (session.isRoot()) {
            RouterHolder.getRouter().executeDatabaseStatement(db, session, this);
        }
    }
}
