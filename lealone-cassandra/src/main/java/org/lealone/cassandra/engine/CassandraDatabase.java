/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.cassandra.engine;

import java.sql.Connection;

import org.lealone.api.ErrorCode;
import org.lealone.cassandra.dbobject.table.CassandraTableEngine;
import org.lealone.dbobject.User;
import org.lealone.engine.ConnectionInfo;
import org.lealone.engine.Database;
import org.lealone.engine.DatabaseEngine;
import org.lealone.message.DbException;

public class CassandraDatabase extends Database {

    public CassandraDatabase(DatabaseEngine dbEngine) {
        super(dbEngine, false);
    }

    @Override
    public String getTableEngineName() {
        return CassandraTableEngine.NAME;
    }

    @Override
    public void init(ConnectionInfo ci, String cipher) {
        setCloseDelay(-1); //session关闭时不马上关闭数据库
        super.init(ci, cipher);
    }

    @Override
    protected CassandraSession createSystemSession(User user, int id) {
        return new CassandraSession(this, user, id);
    }

    @Override
    public synchronized CassandraSession createSession(User user) {
        if (exclusiveSession != null) {
            throw DbException.get(ErrorCode.DATABASE_IS_IN_EXCLUSIVE_MODE);
        }
        CassandraSession session = new CassandraSession(this, user, ++nextSessionId);
        userSessions.add(session);
        trace.info("connecting session #{0} to {1}", session.getId(), databaseName);
        if (delayedCloser != null) {
            delayedCloser.reset();
            delayedCloser = null;
        }
        return session;
    }

    @Override
    public Connection getLobConnection() {
        return null;
    }

    @Override
    public boolean isMultiThreaded() {
        return true; //Cassandra总是使用多线程
    }
}
