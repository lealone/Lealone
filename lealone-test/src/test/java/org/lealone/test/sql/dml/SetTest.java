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
package org.lealone.test.sql.dml;

import org.junit.Test;
import org.lealone.db.DbSetting;
import org.lealone.db.session.SessionSetting;
import org.lealone.test.sql.SqlTestBase;

public class SetTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        System.out.println("DbSetting size: " + DbSetting.values().length);

        testSessionSet();
        testDatabaseSet();

        try {
            sql = "SET unknownType = 3";
            executeUpdate();
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Syntax error"));
        }
    }

    private void testSessionSet() throws Exception {
        try {
            executeUpdate("SET LOCK_TIMEOUT = -1");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SessionSetting.LOCK_TIMEOUT.getName()));
        }
        executeUpdate("SET LOCK_TIMEOUT = 3000");
        executeUpdate("SET QUERY_TIMEOUT 4000");
        executeUpdate("SET SCHEMA public");
        executeUpdate("CREATE SCHEMA IF NOT EXISTS SetTestSchema AUTHORIZATION " + DEFAULT_USER);
        executeUpdate("SET SCHEMA_SEARCH_PATH public,SetTestSchema");

        executeUpdate("SET THROTTLE 10");

        executeUpdate("SET @v1 1");
        executeUpdate("SET @v2 TO 2");
        executeUpdate("SET @v3 = 3");

        sql = "select @v1, @v2, @v3";
        assertEquals(1, getIntValue(1));
        assertEquals(2, getIntValue(2));
        assertEquals(3, getIntValue(3, true));
    }

    private void testDatabaseSet() throws Exception {
        executeUpdate("SET ALLOW_LITERALS NONE");
        executeUpdate("SET ALLOW_LITERALS ALL");
        executeUpdate("SET ALLOW_LITERALS NUMBERS");
        executeUpdate("SET ALLOW_LITERALS 2");
        try {
            executeUpdate("SET ALLOW_LITERALS 10");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.ALLOW_LITERALS.getName()));
        }

        executeUpdate("SET CACHE_SIZE 1000");

        executeUpdate("SET COLLATION off");
        executeUpdate("SET COLLATION DEFAULT_cn STRENGTH PRIMARY");

        executeUpdate("SET BINARY_COLLATION UNSIGNED");
        executeUpdate("SET BINARY_COLLATION SIGNED");
        try {
            executeUpdate("SET BINARY_COLLATION invalidName");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.BINARY_COLLATION.getName()));
        }

        executeUpdate("SET LOB_COMPRESSION_ALGORITHM NO");
        executeUpdate("SET LOB_COMPRESSION_ALGORITHM LZF");
        executeUpdate("SET LOB_COMPRESSION_ALGORITHM DEFLATE");
        try {
            executeUpdate("SET LOB_COMPRESSION_ALGORITHM UNSUPPORTED");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.LOB_COMPRESSION_ALGORITHM.getName()));
        }

        executeUpdate("SET CREATE_BUILD 12");
        try {
            executeUpdate("SET DATABASE_EVENT_LISTENER 'classNameNotFound'");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.DATABASE_EVENT_LISTENER.getName()));
        }

        executeUpdate("SET DB_CLOSE_DELAY 1000");

        executeUpdate("SET DEFAULT_LOCK_TIMEOUT 1000");

        executeUpdate("SET DEFAULT_TABLE_TYPE MEMORY");
        executeUpdate("SET DEFAULT_TABLE_TYPE CACHED");
        executeUpdate("SET DEFAULT_TABLE_TYPE 0");
        try {
            executeUpdate("SET DEFAULT_TABLE_TYPE 5");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.DEFAULT_TABLE_TYPE.getName()));
        }

        executeUpdate("SET EXCLUSIVE 0");
        executeUpdate("SET IGNORECASE true");

        executeUpdate("SET LOCK_MODE 0");
        executeUpdate("SET MAX_LENGTH_INPLACE_LOB 100");
        executeUpdate("SET MAX_MEMORY_ROWS 100");
        executeUpdate("SET MAX_MEMORY_UNDO 100");
        executeUpdate("SET MAX_OPERATION_MEMORY 100");

        executeUpdate("SET MODE MySQL");
        try {
            executeUpdate("SET MODE UNKNOWN_MODE");
            fail(sql);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(DbSetting.MODE.getName()));
        }

        executeUpdate("SET OPTIMIZE_REUSE_RESULTS 0");
        executeUpdate("SET REFERENTIAL_INTEGRITY 0");
        executeUpdate("SET QUERY_STATISTICS 0");
        executeUpdate("SET QUERY_STATISTICS_MAX_ENTRIES 100");

        executeUpdate("SET TRACE_LEVEL_SYSTEM_OUT 0");
        executeUpdate("SET TRACE_LEVEL_FILE 0");
        executeUpdate("SET TRACE_MAX_FILE_SIZE 1000");

        executeUpdate("SET WRITE_DELAY 1000");
    }
}
