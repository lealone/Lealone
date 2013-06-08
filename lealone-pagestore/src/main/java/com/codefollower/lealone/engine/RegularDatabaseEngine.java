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
package com.codefollower.lealone.engine;

import com.codefollower.lealone.constant.ErrorCode;
import com.codefollower.lealone.engine.ConnectionInfo;
import com.codefollower.lealone.engine.Database;
import com.codefollower.lealone.engine.DatabaseEngineBase;
import com.codefollower.lealone.engine.DatabaseEngineManager;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.jmx.DatabaseInfo;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.store.FileLock;
import com.codefollower.lealone.util.Utils;

public class RegularDatabaseEngine extends DatabaseEngineBase {
    public static final String NAME = "REGULAR";
    private static final RegularDatabaseEngine INSTANCE = new RegularDatabaseEngine();
    static {
        DatabaseEngineManager.registerDatabaseEngine(INSTANCE);
    }

    public static RegularDatabaseEngine getInstance() {
        return INSTANCE;
    }

    private boolean jmx;

    @Override
    public RegularDatabase createDatabase() {
        return new RegularDatabase(this);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected Session createSessionAndValidate(ConnectionInfo ci) {
        try {
            ConnectionInfo backup = null;
            String lockMethodName = ci.getProperty("FILE_LOCK", null);
            int fileLockMethod = FileLock.getFileLockMethod(lockMethodName);
            if (fileLockMethod == FileLock.LOCK_SERIALIZED) {
                // In serialized mode, database instance sharing is not possible
                ci.setProperty("OPEN_NEW", "TRUE");
                try {
                    backup = (ConnectionInfo) ci.clone();
                } catch (CloneNotSupportedException e) {
                    throw DbException.convert(e);
                }
            }
            Session session = openSession(ci);
            validateUserAndPassword(true);
            if (backup != null) {
                session.setConnectionInfo(backup);
            }
            return session;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    @Override
    protected void registerMBean(ConnectionInfo ci, Database database, Session session) {
        if (ci.getProperty("JMX", false)) {
            try {
                Utils.callStaticMethod(DatabaseInfo.class.getName() + ".registerMBean", ci, database);
            } catch (Exception e) {
                database.removeSession(session);
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
            jmx = true;
        }
    }

    @Override
    protected void unregisterMBean(String dbName) {
        if (jmx) {
            try {
                Utils.callStaticMethod(DatabaseInfo.class.getName() + ".unregisterMBean", dbName);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
        }
    }
}
