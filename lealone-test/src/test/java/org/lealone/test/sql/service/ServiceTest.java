/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.Test;
import org.lealone.db.api.ErrorCode;
import org.lealone.test.sql.SqlTestBase;

public class ServiceTest extends SqlTestBase {
    @Test
    public void testService() throws Exception {
        executeUpdate("drop service if exists test_service");
        sql = "create service if not exists test_service (" //
                + " test(name varchar) varchar)" //
                + " implement by '" + ServiceTest.class.getName() + "'";
        executeUpdate(sql);

        sql = "EXECUTE SERVICE test_service test('zhh')";
        executeQuery(sql);

        executeUpdate("CREATE USER IF NOT EXISTS zhh PASSWORD 'zhh'");
        Connection conn = DriverManager.getConnection(getURL("zhh", "zhh"));
        Statement stmt = conn.createStatement();
        try {
            stmt.executeQuery(sql);
            fail();
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1);
        }

        executeUpdate("GRANT EXECUTE ON SERVICE test_service TO zhh");
        stmt.executeQuery(sql);

        executeUpdate("REVOKE EXECUTE ON SERVICE test_service FROM zhh");
        try {
            stmt.executeQuery(sql);
            fail();
        } catch (Exception e) {
            assertErrorCode(e, ErrorCode.NOT_ENOUGH_RIGHTS_FOR_1);
        }
        stmt.close();
        conn.close();
    }

    public String test(String name) {
        return name;
    }
}
