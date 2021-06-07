/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.client;

import java.sql.CallableStatement;
import java.sql.Types;

import org.junit.Test;
import org.lealone.test.sql.SqlTestBase;

public class JdbcCallableStatementTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        init();
        test();
    }

    void init() throws Exception {
        stmt.executeUpdate("CREATE ALIAS IF NOT EXISTS MY_SQRT FOR \"java.lang.Math.sqrt\"");
    }

    @Override
    protected void test() throws Exception {
        sql = "?= CALL MY_SQRT(?)";
        CallableStatement cs = conn.prepareCall(sql);
        cs.registerOutParameter(1, Types.DOUBLE); // sqlType其实被忽略了，所以设什么都没用
        cs.setDouble(2, 4.0);
        cs.execute();

        assertEquals(2.0, cs.getDouble(1), 0.0);
        cs.close();
    }
}