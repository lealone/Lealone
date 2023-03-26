/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.admin;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.test.TestBase.MainTest;
import org.lealone.test.sql.SqlTestBase;

public class ShutdownServerTest extends SqlTestBase implements MainTest {

    public static void main(String[] args) throws Exception {
        new ShutdownServerTest().runTest();
    }

    @Override
    protected void test() throws Exception {
        run();
    }

    @Test
    public void run() {
        try {
            stmt.executeUpdate("shutdown server " + getPort());
            fail();
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.LEALONE_DATABASE_ADMIN_RIGHT_1);
        }
    }
}
