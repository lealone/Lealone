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
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.api.ErrorCode;
import org.lealone.common.message.JdbcSQLException;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class CreateDatabaseTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest1");
        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest2 TEMPORARY");

        try {
            stmt.executeUpdate("CREATE DATABASE CreateDatabaseTest1");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof JdbcSQLException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((JdbcSQLException) e).getErrorCode());
        }

        try {
            stmt.executeUpdate("CREATE DATABASE " + LealoneDatabase.NAME);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof JdbcSQLException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((JdbcSQLException) e).getErrorCode());
        }
    }
}
