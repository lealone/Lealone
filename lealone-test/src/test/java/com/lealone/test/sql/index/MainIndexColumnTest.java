/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.index;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class MainIndexColumnTest extends SqlTestBase {

    @Test
    public void run() throws Exception {
        executeUpdate("drop table IF EXISTS MainIndexColumnTest CASCADE");
        executeUpdate(
                "create table IF NOT EXISTS MainIndexColumnTest(id int not null, name varchar(50))");

        executeUpdate(
                "CREATE PRIMARY KEY IF NOT EXISTS MainIndexColumnTest_id ON MainIndexColumnTest(id)");

        executeUpdate("insert into MainIndexColumnTest(id, name) values(10, 'a1')");
        executeUpdate("insert into MainIndexColumnTest(id, name) values(20, 'b1')");
        executeUpdate("insert into MainIndexColumnTest(id, name) values(30, 'a2')");

        sql = "select * from MainIndexColumnTest";
        printResultSet();

        sql = "select min(id),max(id) from MainIndexColumnTest";
        assertEquals(10, getIntValue(1, false));
        assertEquals(30, getIntValue(2, true));
    }

}
