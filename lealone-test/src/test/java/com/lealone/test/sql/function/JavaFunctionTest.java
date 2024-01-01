/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.function;

import java.sql.ResultSet;
import java.sql.Types;

import org.junit.Test;

import com.lealone.db.result.SimpleResultSet;
import com.lealone.test.sql.SqlTestBase;

public class JavaFunctionTest extends SqlTestBase {
    @Test
    public void run() throws Exception {
        executeUpdate("CREATE ALIAS IF NOT EXISTS getResultSet DETERMINISTIC FOR " + "\""
                + JavaFunctionTest.class.getName() + ".getResultSet\"");
        sql = "select * from getResultSet()";
        assertEquals(2, printResultSet());
    }

    public static ResultSet getResultSet() {
        SimpleResultSet rs = new SimpleResultSet();
        rs.addColumn("f1", Types.INTEGER, Integer.MAX_VALUE, 0);
        rs.addColumn("f2", Types.INTEGER, Integer.MAX_VALUE, 0);
        rs.addRow(1, 2);
        rs.addRow(10, 20);
        return rs;
    }
}
