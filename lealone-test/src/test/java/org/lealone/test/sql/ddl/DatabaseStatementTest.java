/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.ddl;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class DatabaseStatementTest extends SqlTestBase {

    public DatabaseStatementTest() {
        super(LealoneDatabase.NAME);
    }

    private void printException(Exception e) {
        System.out.println(getRootCause(e).getMessage());
    }

    @Test
    public void run() throws Exception {
        // 不能删除lealone数据库
        try {
            executeUpdate("DROP DATABASE " + LealoneDatabase.NAME);
            fail();
        } catch (Exception e) {
            printException(e);
        }

        executeUpdate("CREATE DATABASE IF NOT EXISTS DatabaseStatementTest");

        new NoRightTest("DatabaseStatementTest").runTest();

        executeUpdate("DROP DATABASE IF EXISTS DatabaseStatementTest DELETE FILES");
    }

    private class NoRightTest extends SqlTestBase {

        public NoRightTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            // 只有以管理员身份连到lealone数据库时才能执行CREATE/ALTER/DROP DATABASE语句
            try {
                executeUpdate("CREATE DATABASE IF NOT EXISTS db1");
                fail();
            } catch (Exception e) {
                printException(e);
            }

            try {
                executeUpdate("ALTER DATABASE DatabaseStatementTest RUN MODE REPLICATION");
                fail();
            } catch (Exception e) {
                printException(e);
            }

            try {
                executeUpdate("DROP DATABASE IF EXISTS DatabaseStatementTest");
                fail();
            } catch (Exception e) {
                printException(e);
            }
        }
    }
}
