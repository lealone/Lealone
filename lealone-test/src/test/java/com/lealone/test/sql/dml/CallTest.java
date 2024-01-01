/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.dml;

import org.junit.Test;

import com.lealone.test.sql.SqlTestBase;

public class CallTest extends SqlTestBase {
    @Test
    public void run() {
        executeUpdate("drop table IF EXISTS CallTest");
        executeUpdate("create table IF NOT EXISTS CallTest(id int, name varchar(500), b boolean)");

        executeUpdate("insert into CallTest(id, name, b) values(1, 'a1', true)");
        executeUpdate("insert into CallTest(id, name, b) values(2, 'a2', false)");
        executeUpdate("insert into CallTest(id, name, b) values(3, 'a3', false)");

        sql = "CALL select * from CallTest where id=1";
        sql = "CALL TABLE(ID INT=(1, 2), NAME VARCHAR=('Hello', 'World'))";
        printResultSet();

        sql = "CALL CURTIME()";
        printResultSet();

        sql = "CALL ABS(-100)";
        System.out.println(executeUpdate());
    }
}
