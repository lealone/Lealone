/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.admin;

import org.lealone.test.TestBase.MainTest;
import org.lealone.test.sql.SqlTestBase;

public class ShutdownServerTest extends SqlTestBase implements MainTest {

    public static void main(String[] args) throws Exception {
        new ShutdownServerTest().runTest();
    }

    // @Test //不用这个，在做集成测试时会影响其他测试
    @Override
    protected void test() throws Exception {
        stmt.executeUpdate("admin shutdown server " + getPort());
    }
}
