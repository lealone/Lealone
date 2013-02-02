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
package com.codefollower.yourbase.engine;

import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Engine;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.store.FileLock;

public class RegularEngine extends Engine {
    private static final RegularEngine INSTANCE = new RegularEngine();

    public static RegularEngine getInstance() {
        return INSTANCE;
    }

    @Override
    public Session createSession(ConnectionInfo ci) {
        return INSTANCE.createSessionAndValidate(ci);
    }

    @Override
    protected RegularDatabase createDatabase() {
        return new RegularDatabase();
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
}
