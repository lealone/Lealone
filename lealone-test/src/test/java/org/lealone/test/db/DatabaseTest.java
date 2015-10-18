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
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.result.SearchRow;

public class DatabaseTest extends DbObjectTestBase {

    private void asserts(String dbName) {
        int id;
        Database db;

        db = findDatabase(dbName);
        assertNotNull(db);
        id = db.getId();
        assertTrue(id > 0);
        SearchRow row = LealoneDatabase.getInstance().findMeta(session, id);
        if (db.isPersistent()) {// 只有非内存数据库才会在meta表中保存一条记录用来代表它
            assertNotNull(row);
            assertEquals(id, row.getValue(0).getInt());
        } else
            assertNull(row);
    }

    @Test
    public void run() {
        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest1");
        asserts("CreateDatabaseTest1");

        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest2 WITH(OPTIMIZE_DISTINCT=true, PERSISTENT=false)");
        asserts("CreateDatabaseTest2");

        executeUpdate("CREATE DATABASE IF NOT EXISTS CreateDatabaseTest3 WITH()");
        asserts("CreateDatabaseTest3");

        try {
            executeUpdate("CREATE DATABASE CreateDatabaseTest1");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof DbException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((DbException) e).getErrorCode());
        }

        try {
            executeUpdate("CREATE DATABASE " + LealoneDatabase.NAME);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof DbException);
            assertEquals(ErrorCode.DATABASE_ALREADY_EXISTS_1, ((DbException) e).getErrorCode());
        }
    }
}
