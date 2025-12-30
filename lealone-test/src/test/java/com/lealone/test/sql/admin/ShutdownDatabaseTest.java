/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.admin;

import java.sql.Connection;

import org.junit.Test;

import com.lealone.db.ConnectionSetting;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.api.ErrorCode;
import com.lealone.test.sql.SqlTestBase;

public class ShutdownDatabaseTest extends SqlTestBase {

    public ShutdownDatabaseTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void run() throws Exception {
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest1");
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest2");
        executeUpdate("CREATE DATABASE IF NOT EXISTS ShutdownDatabaseTest3");

        ShutdownTest t1 = new ShutdownTest("ShutdownDatabaseTest1");
        // 用专有连接测试，否则集成测试时有问题
        t1.addConnectionParameter(ConnectionSetting.IS_SHARED, "false");
        Connection c1 = t1.getConnection();
        ShutdownTest t2 = new ShutdownTest("ShutdownDatabaseTest2");
        t2.addConnectionParameter(ConnectionSetting.IS_SHARED, "false");
        Connection c2 = t2.getConnection();

        executeUpdate("SHUTDOWN DATABASE ShutdownDatabaseTest1");
        executeUpdate("SHUTDOWN DATABASE ShutdownDatabaseTest2 IMMEDIATELY");
        try {
            c1.setSavepoint("s1");
            fail();
        } catch (Exception e) {
            // 会产生各种可能的错误，比如ErrorCode.IO_EXCEPTION_1, ErrorCode.CONNECTION_BROKEN_1
        }
        try {
            c2.setSavepoint("s2");
            fail();
        } catch (Exception e) {
        }
        new ShutdownRightTest("ShutdownDatabaseTest3").runTest();
    }

    private static class ShutdownRightTest extends SqlTestBase {
        public ShutdownRightTest(String dbName) {
            super(dbName);
        }

        @Override
        protected void test() throws Exception {
            try {
                executeUpdate("SHUTDOWN DATABASE " + dbName);
                fail();
            } catch (Exception e) {
                assertErrorCode(e, ErrorCode.LEALONE_DATABASE_ADMIN_RIGHT_1);
            }
        }
    }

    private static class ShutdownTest extends SqlTestBase {
        public ShutdownTest(String dbName) {
            super(dbName);
        }
    }
}
