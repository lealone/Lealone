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
package org.lealone.test.db;

import org.junit.Test;
import org.lealone.api.ErrorCode;
import org.lealone.common.message.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.UnitTestBase;

public class DatabaseEngineTest extends UnitTestBase {

    @Test
    public void run() {
        initTransactionEngine();

        setInMemory(true);
        setEmbedded(true);

        ConnectionInfo ci;
        try {
            ci = new ConnectionInfo(getURL(LealoneDatabase.NAME));
            DatabaseEngine.createSession(ci);
            fail();
        } catch (DbException e) {
            assertEquals(ErrorCode.DATABASE_NOT_FOUND_1, e.getErrorCode());
        }

        try {
            ci = new ConnectionInfo(getURL("wrong_user", ""));
            DatabaseEngine.createSession(ci);
            fail();
        } catch (DbException e) {
            assertEquals(ErrorCode.WRONG_USER_OR_PASSWORD, e.getErrorCode());
        }

        try {
            ci = new ConnectionInfo(getURL("sa", "wrong_password"));
            DatabaseEngine.createSession(ci);
            fail();
        } catch (DbException e) {
            assertEquals(ErrorCode.WRONG_USER_OR_PASSWORD, e.getErrorCode());
        }

        ci = new ConnectionInfo(getURL("wrong_user", ""));
        ci.disableAuthentication();
        DatabaseEngine.createSession(ci);
    }
}
